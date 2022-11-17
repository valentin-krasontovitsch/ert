from ert._c_wrappers.enkf import ResConfig

config_file_path = "/tmp/config.ert"

contents = "NUM_REALIZATIONS 1\n"

with open(config_file_path, mode="w") as fh:
    fh.write(contents)

ResConfig(user_config_file=config_file_path)
