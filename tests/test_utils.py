
from wgs_sketcher.utils import is_target_file, shard_subdir_for

def test_is_target_file():
    assert is_target_file("wgs.AASDTR.1.fsa_nt.gz")
    assert is_target_file("wgs.BCDE01.2.fsa_nt.gz")
    assert is_target_file("wgs.QWERTY.fsa_nt.gz")
    assert not is_target_file("wgs.AASDTR.1.gbff.gz")
    assert not is_target_file("stats.wgs.AASDTR/file.txt")

def test_shard_subdir_for():
    assert shard_subdir_for("wgs.AASDTR.1.fsa_nt.gz") == "AAS"
    assert shard_subdir_for("wgs.CADASZ.1.fsa_nt.gz") == "CAD"
