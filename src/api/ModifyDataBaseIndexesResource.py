from flask_restful import Resource

from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.CandidatesFollowersDAO import CandidatesFollowersDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO


class ModifyDataBaseIndexesResource(Resource):

    @staticmethod
    def patch():
        # TODO: Delete this after use
        CandidateDAO().replace_ids_for_screen_names()
        RawFollowerDAO().replace_ids_for_twitter_id()
        CandidatesFollowersDAO().replace_ids_for_screen_names()
