'''
author: Sarthak Khandelwal
'''

class Data:
    """Data class for data present at each site. Each site has it's own copy of data objects."""

    def __init__(self, variable_name: str, value: int):
        """Set a data object with a name and value."""
        self.variableName = variable_name
        self.value = value
        self.lastWriteTime = 0 

    def setValue(self, new_value: int, tick: int):
        """Update a data object's value"""
        self.value = new_value
        self.lastWriteTime = tick  # Update timestamp -> we may need a global ticker or probably pass time in the parameters

    def getValue(self) -> int:
        """Get a data object's value"""
        return self.value
    

