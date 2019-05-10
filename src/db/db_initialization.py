from src.db.dao.CandidateDAO import CandidateDAO


def create_indexes():
    # Create all required collection indexes
    CandidateDAO().create_indexes()


def create_base_entries():
    # Create all required entries
    CandidateDAO().create_base_entries()
