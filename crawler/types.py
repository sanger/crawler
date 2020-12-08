from datetime import datetime
from typing import Any, Dict, Union

Sample = Dict[str, Any]  # Type alias for a mongo document that represents a sample
SourcePlate = Dict[str, Union[str, datetime]]  # Type alias for a mongo document that represents a source plate
DartWellProp = Dict[str, str]  # Type alias for the well properties of a DART well 'object'
