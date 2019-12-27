import pytest
import pyppl_strict


def test_strict():
	assert pyppl_strict.RC_NO_OUTFILE == 5000
	assert pyppl_strict.RC_EXPECT_FAIL == 10000