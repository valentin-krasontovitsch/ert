import os
from typing import List, Optional

from cwrap import BaseCClass
from ecl.grid import EclGrid
from ecl.summary import EclSum
from ecl.util.util import StringList

from ert import _clib
from ert._c_wrappers import ResPrototype
from ert._c_wrappers.config import ConfigContent
from ert._c_wrappers.enkf.config import EnkfConfigNode
from ert._c_wrappers.enkf.config_keys import ConfigKeys
from ert._c_wrappers.enkf.enums import EnkfVarType, ErtImplType


def _get_abs_path(file):
    if file is not None:
        file = os.path.realpath(file)
    return file


def _get_filename(file):
    if file is not None:
        file = os.path.basename(file)
    return file


class EnsembleConfig(BaseCClass):
    TYPE_NAME = "ens_config"
    _alloc = ResPrototype(
        "void* ensemble_config_alloc(config_content, ecl_grid, ecl_sum)", bind=False
    )
    _alloc_full = ResPrototype("void* ensemble_config_alloc_full(char*)", bind=False)
    _free = ResPrototype("void ensemble_config_free( ens_config )")
    _has_key = ResPrototype("bool ensemble_config_has_key( ens_config , char* )")
    _size = ResPrototype("int ensemble_config_get_size( ens_config)")
    _get_node = ResPrototype(
        "enkf_config_node_ref ensemble_config_get_node( ens_config , char*)"
    )
    _alloc_keylist = ResPrototype(
        "stringlist_obj ensemble_config_alloc_keylist( ens_config )"
    )
    _add_summary = ResPrototype(
        "enkf_config_node_ref ensemble_config_add_summary( ens_config, char*, int)"
    )
    _add_gen_kw = ResPrototype(
        "enkf_config_node_ref ensemble_config_add_gen_kw( ens_config, char*)"
    )
    _add_field = ResPrototype(
        "enkf_config_node_ref ensemble_config_add_field( ens_config, char*, ecl_grid)"
    )
    _alloc_keylist_from_impl_type = ResPrototype(
        "stringlist_obj ensemble_config_alloc_keylist_from_impl_type(ens_config, \
                                                                     ert_impl_type_enum)"  # noqa
    )
    _add_node = ResPrototype(
        "void ensemble_config_add_node( ens_config , enkf_config_node )"
    )
    _get_trans_table = ResPrototype("void* ensemble_config_get_trans_table(ens_config)")
    _add_summary_full = ResPrototype(
        "void ensemble_config_init_SUMMARY_full(ens_config, char*, ecl_sum)"
    )

    def __init__(
        self,
        config_content: Optional[ConfigContent] = None,
        grid: Optional[EclGrid] = None,
        refcase: Optional[EclSum] = None,
        config_dict=None,
    ):
        if config_content is not None and config_dict is not None:
            raise ValueError(
                "Attempting to create EnsembleConfig "
                "object with multiple config objects"
            )

        c_ptr = None
        if config_dict is not None:
            c_ptr = self._alloc_full(config_dict.get(ConfigKeys.GEN_KW_TAG_FORMAT))
            if c_ptr is None:
                raise ValueError(
                    "Failed to construct EnsembleConfig instance from dict"
                )

            super().__init__(c_ptr)

            gen_data_list = config_dict.get(ConfigKeys.GEN_DATA, [])
            for gene_data in gen_data_list:
                gen_data_node = EnkfConfigNode.create_gen_data_full(
                    gene_data.get(ConfigKeys.NAME),
                    gene_data.get(ConfigKeys.RESULT_FILE),
                    gene_data.get(ConfigKeys.INPUT_FORMAT),
                    gene_data.get(ConfigKeys.REPORT_STEPS),
                    gene_data.get(ConfigKeys.ECL_FILE),
                    gene_data.get(ConfigKeys.INIT_FILES),
                    gene_data.get(ConfigKeys.TEMPLATE),
                    gene_data.get(ConfigKeys.KEY_KEY),
                )
                self.addNode(gen_data_node)

            gen_kw_list = config_dict.get(ConfigKeys.GEN_KW, [])
            for gen_kw in gen_kw_list:
                gen_kw_node = EnkfConfigNode.create_gen_kw(
                    gen_kw.get(ConfigKeys.NAME),
                    _get_abs_path(gen_kw.get(ConfigKeys.TEMPLATE)),
                    gen_kw.get(ConfigKeys.OUT_FILE),
                    _get_abs_path(gen_kw.get(ConfigKeys.PARAMETER_FILE)),
                    gen_kw.get(ConfigKeys.FORWARD_INIT),
                    gen_kw.get(ConfigKeys.INIT_FILES),
                    config_dict.get(ConfigKeys.GEN_KW_TAG_FORMAT),
                )
                self.addNode(gen_kw_node)

            surface_list = config_dict.get(ConfigKeys.SURFACE_KEY, [])
            for surface in surface_list:
                surface_node = EnkfConfigNode.create_surface(
                    surface.get(ConfigKeys.NAME),
                    surface.get(ConfigKeys.INIT_FILES),
                    surface.get(ConfigKeys.OUT_FILE),
                    surface.get(ConfigKeys.BASE_SURFACE_KEY),
                    surface.get(ConfigKeys.FORWARD_INIT),
                )
                self.addNode(surface_node)

            summary_list = config_dict.get(ConfigKeys.SUMMARY, [])
            for a in summary_list:
                self.add_summary_full(a, refcase)

            field_list = config_dict.get(ConfigKeys.FIELD_KEY, [])
            for field in field_list:
                field_node = EnkfConfigNode.create_field(
                    field.get(ConfigKeys.NAME),
                    field.get(ConfigKeys.VAR_TYPE),
                    grid,
                    self._get_trans_table(),
                    field.get(ConfigKeys.OUT_FILE),
                    field.get(ConfigKeys.FORWARD_INIT),
                    field.get(ConfigKeys.INIT_TRANSFORM),
                    field.get(ConfigKeys.OUTPUT_TRANSFORM),
                    field.get(ConfigKeys.INPUT_TRANSFORM),
                    field.get(ConfigKeys.MIN_KEY),
                    field.get(ConfigKeys.MAX_KEY),
                    field.get(ConfigKeys.INIT_FILES),
                )
                self.addNode(field_node)

            schedule_file_list = config_dict.get(
                ConfigKeys.SCHEDULE_PREDICTION_FILE, []
            )
            for schedule_file in schedule_file_list:
                schedule_file_node = EnkfConfigNode.create_gen_kw(
                    ConfigKeys.PRED_KEY,
                    schedule_file.get(ConfigKeys.TEMPLATE),
                    _get_filename(schedule_file.get(ConfigKeys.TEMPLATE)),
                    schedule_file.get(ConfigKeys.PARAMETER_KEY),
                    False,
                    schedule_file.get(ConfigKeys.INIT_FILES),
                    config_dict.get(ConfigKeys.GEN_KW_TAG_FORMAT),
                )
                self.addNode(schedule_file_node)

            return

        c_ptr = self._alloc(config_content, grid, refcase)
        if c_ptr is None:
            raise ValueError("Failed to construct EnsembleConfig instance")
        super().__init__(c_ptr)

    def __len__(self):
        return self._size()

    def __repr__(self):
        if not self._address():
            return "<EnsembleConfig()>"
        return (
            "EnsembleConfig(config_dict={"
            + f"{ConfigKeys.GEN_KW}: ["
            + ", ".join(
                f"{self.getNode(key)}"
                for key in self.alloc_keylist()
                if self.getNode(key).getImplementationType() == ErtImplType.GEN_KW
            )
            + "], "
            + f"{ConfigKeys.GEN_DATA}: ["
            + ", ".join(
                f"{self.getNode(key)}"
                for key in self.alloc_keylist()
                if self.getNode(key).getImplementationType() == ErtImplType.GEN_DATA
                and self.getNode(key).getVariableType() == EnkfVarType.DYNAMIC_RESULT
            )
            + "], "
            + f"{ConfigKeys.SURFACE_KEY}: ["
            + ", ".join(
                f"{self.getNode(key)}"
                for key in self.alloc_keylist()
                if self.getNode(key).getImplementationType() == ErtImplType.SURFACE
            )
            + "], "
            + f"{ConfigKeys.SUMMARY}: ["
            + ", ".join(
                f"{self.getNode(key)}"
                for key in self.alloc_keylist()
                if self.getNode(key).getImplementationType() == ErtImplType.SUMMARY
            )
            + "], "
            + f"{ConfigKeys.FIELD_KEY}: ["
            + ", ".join(
                f"{self.getNode(key)}"
                for key in self.alloc_keylist()
                if self.getNode(key).getImplementationType() == ErtImplType.FIELD
            )
            + "]"
            + "}"
        )

    def __getitem__(self, key: str) -> EnkfConfigNode:
        if key in self:
            return self._get_node(key).setParent(self)
        else:
            raise KeyError(f"The key:{key} is not in the ensemble configuration")

    def getNode(self, key: str) -> EnkfConfigNode:
        return self[key]

    def alloc_keylist(self) -> StringList:
        return self._alloc_keylist()

    def add_summary(self, key) -> EnkfConfigNode:
        return self._add_summary(key, 2).setParent(self)

    def add_summary_full(self, key, refcase) -> EnkfConfigNode:
        return self._add_summary_full(key, refcase)

    def add_gen_kw(self, key) -> EnkfConfigNode:
        return self._add_gen_kw(key).setParent(self)

    def addNode(self, config_node: EnkfConfigNode):
        assert isinstance(config_node, EnkfConfigNode)
        self._add_node(config_node)
        config_node.convertToCReference(self)

    def add_field(self, key, eclipse_grid: EclGrid) -> EnkfConfigNode:
        return self._add_field(key, eclipse_grid).setParent(self)

    def getKeylistFromVarType(self, var_mask: EnkfVarType) -> List[str]:
        assert isinstance(var_mask, EnkfVarType)
        return _clib.ensemble_config.ensemble_config_keylist_from_var_type(
            self, int(var_mask)
        )

    def getKeylistFromImplType(self, ert_impl_type) -> List[str]:
        assert isinstance(ert_impl_type, ErtImplType)
        return list(self._alloc_keylist_from_impl_type(ert_impl_type))

    @property
    def parameters(self) -> List[str]:
        return self.getKeylistFromVarType(EnkfVarType.PARAMETER)

    def __contains__(self, key):
        return self._has_key(key)

    def free(self):
        self._free()

    def __eq__(self, other):
        self_param_list = set(self.alloc_keylist())
        other_param_list = set(other.alloc_keylist())
        if self_param_list != other_param_list:
            return False

        for a in self_param_list:
            if a in self and a in other:
                if self.getNode(a) != other.getNode(a):
                    return False
            else:
                return False

        return True

    def __ne__(self, other):
        return not self == other
