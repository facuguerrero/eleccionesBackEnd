from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO


def create_indexes():
    # Create all required collection indexes
    CandidateDAO().create_indexes()
    RawFollowerDAO().create_indexes()


def create_base_entries():
    # Create all required entries
    CandidateDAO().create_base_entries()
