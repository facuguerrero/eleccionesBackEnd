from src.service.followers.FollowerSupportService import FollowerSupportService
from test.meta.CustomTestCase import CustomTestCase


class TestFollowerSupportService(CustomTestCase):

    def test_with_both_vectors(self):
        rt_vector = [40, 0, 0, 0, 0]
        follows_vector = [1, 0, 0, 0]

        rt_result, follows_result = FollowerSupportService.get_final_vectors(rt_vector, follows_vector)

        assert int(sum(rt_result) + sum(follows_result)) == 1

    def test_with_both_vectors_difderents(self):
        rt_vector = [40, 0, 0, 0, 0]
        follows_vector = [0, 0, 1, 0]

        rt_result, follows_result = FollowerSupportService.get_final_vectors(rt_vector, follows_vector)

        assert int(sum(rt_result) + sum(follows_result)) == 1
        assert rt_result[0] == 0.8

    def test_with_follows_vector(self):
        rt_vector = [0, 0, 0, 0, 0]
        follows_vector = [1, 0, 0, 0]

        rt_result, follows_result = FollowerSupportService.get_final_vectors(rt_vector, follows_vector)

        assert int(sum(rt_result) + sum(follows_result)) == 1

    def test_with_rt_vector(self):
        rt_vector = [40, 0, 0, 0, 0]
        follows_vector = [0, 0, 0, 0]

        rt_result, follows_result = FollowerSupportService.get_final_vectors(rt_vector, follows_vector)

        assert int(sum(rt_result) + sum(follows_result)) == 1

    def test_with_no_vector(self):
        rt_vector = [0, 0, 0, 0, 0]
        follows_vector = [0, 0, 0, 0]

        rt_result, follows_result = FollowerSupportService.get_final_vectors(rt_vector, follows_vector)

        assert int(sum(rt_result) + sum(follows_result)) == 0
