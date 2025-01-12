#include <ert/util/test_util.hpp>
#include <ert/util/rng.h>

#include <ert/res_util/es_testdata.hpp>

#include <ert/analysis/ies/ies_data.hpp>
#include <ert/analysis/ies/ies.hpp>

/*
TEST 3 (consistency between IES and STD_ENKF):
 - ANALYSIS_SET_VAR IES_ENKF ENKF_TRUNCATION         0.999
 - ANALYSIS_SET_VAR IES_ENKF IES_STEPLENGTH          1.0
 - ANALYSIS_SET_VAR IES_ENKF IES_INVERSION           1
 - ANALYSIS_SET_VAR IES_ENKF IES_AAPROJECTION        false
should give same result as:
 - ANALYSIS_SET_VAR IES_ENKF ENKF_TRUNCATION         0.999
 - ANALYSIS_SELECT STD_ENKF
*/

void cmp_std_ies(const res::es_testdata &testdata) {
    rng_type *rng = rng_alloc(MZRAN, INIT_DEFAULT);
    Eigen::MatrixXd A1 = testdata.make_state("prior");
    Eigen::MatrixXd A2 = testdata.make_state("prior");
    ies::data::Data ies_data1(testdata.active_ens_size);
    ies::config::Config ies_config1(true);
    ies::config::Config std_config(false);

    ies_config1.truncation(0.95);
    ies_config1.min_steplength(1.0);
    ies_config1.max_steplength(1.0);
    ies_config1.inversion(ies::config::IES_INVERSION_SUBSPACE_EXACT_R);
    ies_config1.aaprojection(false);

    std_config.truncation(0.95);

    ies::init_update(ies_data1, testdata.ens_mask, testdata.obs_mask);

    ies::updateA(ies_config1, ies_data1, A1, testdata.S, testdata.R, testdata.E,
                 testdata.D);

    int active_ens_size = A2.cols();
    Eigen::MatrixXd W0 =
        Eigen::MatrixXd::Zero(active_ens_size, active_ens_size);
    Eigen::MatrixXd X = ies::makeX({}, testdata.S, testdata.R, testdata.E,
                                   testdata.D, std_config.inversion(),
                                   std_config.truncation(), false, W0, 1, 1);

    A2 *= X;
    test_assert_true(A1.isApprox(A2, 5e-6));

    rng_free(rng);
}

int main(int argc, char **argv) {
    res::es_testdata testdata(argv[1]);
    cmp_std_ies(testdata);
}
