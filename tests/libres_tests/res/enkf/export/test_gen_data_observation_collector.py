from libres_utils import ResTest

from res.enkf.export import GenDataObservationCollector
from res.test import ErtTestContext


class GenDataObservationCollectorTest(ResTest):
    def test_gen_data_collector(self):
        config = self.createTestPath("local/mini_ert/mini_config")
        with ErtTestContext(
            "python/enkf/export/gen_data_observation_collector", config
        ) as context:
            ert = context.getErt()

            obs_key = GenDataObservationCollector.getObservationKeyForDataKey(
                ert, "PERLIN", 1
            )
            self.assertEqual(obs_key, "GEN_PERLIN_1")

            obs_key = GenDataObservationCollector.getObservationKeyForDataKey(
                ert, "PERLIN", 2
            )
            self.assertEqual(obs_key, "GEN_PERLIN_2")

            obs_key = GenDataObservationCollector.getObservationKeyForDataKey(
                ert, "PERLIN", 3
            )
            self.assertEqual(obs_key, "GEN_PERLIN_3")

            obs_key = GenDataObservationCollector.getObservationKeyForDataKey(
                ert, "PERLIN", 4
            )
            self.assertIsNone(obs_key)

            obs_key = GenDataObservationCollector.getObservationKeyForDataKey(
                ert, "PERLINk", 1
            )
            self.assertIsNone(obs_key)
