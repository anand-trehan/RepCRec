#!/bin/bash

# Path to the tests folder
TEST_FOLDER="tests"

# Check if the tests folder exists
if [ ! -d "$TEST_FOLDER" ]; then
  echo "Error: Folder '$TEST_FOLDER' does not exist."
  exit 1
fi

# Loop through all test files in the folder
for TEST_FILE in "$TEST_FOLDER"/test*.txt; do
  if [ -f "$TEST_FILE" ]; then
    # Extract the test number from the file name
    TEST_NUMBER=$(basename "$TEST_FILE" | sed -E 's/test([0-9]+)\.txt/\1/')
    
    echo "************ TEST $TEST_NUMBER ************"
    python3 main.py "$TEST_FILE"
    echo "************ END OF TEST $TEST_NUMBER ************"
    echo
  fi
done

echo "All tests completed."
