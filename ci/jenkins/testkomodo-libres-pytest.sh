copy_test_files () {
    cp -r ${CI_SOURCE_ROOT}/tests ${CI_TEST_ROOT}
    ln -s ${CI_SOURCE_ROOT}/test-data ${CI_TEST_ROOT}/test-data

    # Trick to find a fake source root
    mkdir ${CI_TEST_ROOT}/.git

    mkdir -p ${CI_TEST_ROOT}/libres/res/fm/rms
    ln -s ${CI_SOURCE_ROOT}/res/fm/rms/rms_config.yml ${CI_TEST_ROOT}/libres/res/fm/rms/rms_config.yml
    ln -s {$CI_SOURCE_ROOT,$CI_TEST_ROOT}/libres/lib
    ln -s {$CI_SOURCE_ROOT,$CI_TEST_ROOT}/libres/bin

    ln -s ${CI_SOURCE_ROOT}/share ${CI_TEST_ROOT}/share
}

install_test_dependencies () {
    pip install -r dev-requirements.txt
}

install_package () {
    pip install . --no-deps
}

start_tests () {
    pushd ${CI_TEST_ROOT}/tests/libres_tests
    ln -s /project/res-testdata/ErtTestData ${CI_TEST_ROOT}/test-data/Equinor
    export ECL_SKIP_SIGNAL=ON
    pytest                                                   \
        --ignore="tests/libres_tests/res/enkf/test_analysis_config.py"    \
        --ignore="tests/libres_tests/res/enkf/test_res_config.py"         \
        --ignore="tests/libres_tests/res/enkf/test_site_config.py"        \
        --ignore="tests/libres_tests/res/enkf/test_workflow_list.py"      \
        --ignore="tests/libres_tests/res/enkf/test_hook_manager.py"

    popd
}
