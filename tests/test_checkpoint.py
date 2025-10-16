import os
import pickle
import pandas as pd
import pytest

import adaptive_search.checkpoint as checkpoint


def test_save_and_load_checkpoint(tmp_path, monkeypatch):
    # Change working directory to a temp path
    monkeypatch.chdir(tmp_path)

    state = {'a': 1, 'b': [1, 2, 3]}
    # Save checkpoint
    checkpoint.save_checkpoint(state)
    # Check that checkpoint file exists
    ckpt_file = tmp_path / checkpoint.CKPT_FILE
    assert ckpt_file.exists(), "Checkpoint file was not created"

    # Load checkpoint and verify content
    loaded = checkpoint.load_checkpoint()
    assert loaded == state, "Loaded checkpoint does not match saved state"


def test_load_checkpoint_no_file(tmp_path, monkeypatch):
    # Ensure no checkpoint file exists
    monkeypatch.chdir(tmp_path)
    # Attempt to load without saving
    result = checkpoint.load_checkpoint()
    assert result is None, "Expected None when no checkpoint file exists"


def test_flush_chunk_empty_buffer(tmp_path, monkeypatch):
    # Change working directory to a temp path
    monkeypatch.chdir(tmp_path)

    buffer = []
    # Should not raise and not create any file
    checkpoint.flush_chunk('TestCity', buffer)
    expected_file = tmp_path / 'testcity_restaurants.csv'
    assert not expected_file.exists(), "File should not be created for empty buffer"


def test_flush_chunk_writes_and_appends(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # First chunk
    buffer = [
        {'name': 'A', 'rating': 5},
        {'name': 'B', 'rating': 4},
    ]
    checkpoint.flush_chunk('SampleCity', buffer)
    csv_file = tmp_path / 'samplecity_restaurants.csv'
    assert csv_file.exists(), "CSV file should be created after flush"

    # Read and validate content
    df = pd.read_csv(csv_file)
    assert list(df.columns) == ['name', 'rating'], "CSV header mismatch"
    assert len(df) == 2
    assert df.iloc[0].to_dict() == {'name': 'A', 'rating': 5}
    assert df.iloc[1].to_dict() == {'name': 'B', 'rating': 4}

    # Buffer should be cleared
    assert buffer == [], "Buffer was not cleared after flush"

    # Append another chunk
    buffer.extend([{'name': 'C', 'rating': 3}])
    checkpoint.flush_chunk('SampleCity', buffer)

    # Read again and validate append
    df2 = pd.read_csv(csv_file)
    assert len(df2) == 3, "CSV should have three rows after appending"
    assert df2.iloc[2].to_dict() == {'name': 'C', 'rating': 3}
    # Buffer cleared again
    assert buffer == []

if __name__ == '__main__':
    pytest.main()
