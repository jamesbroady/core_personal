from enum import Enum

class SystemType(Enum):
    """The type of the system."""
    # Executable in a python environment
    PYTHON = 1
    # Executable in a pyspark environment
    PYSPARK = 2
    # Executable in a databricks environment
    DATABRICKS = 3


class LibraryTypes(Enum):
    MAVEN = 1
    PYPI = 2
    PYTHONWHL = 3

class MavenLibrary():
    group_id: str
    artifact_id: str
    version: str
    repository: str

    def __init__(self, group_id: str, artifact_id: str, version: str, repository: str = None):
        self.group_id = group_id
        self.artifact_id = artifact_id
        self.version = version
        self.repository = repository

    def to_string(self) -> str:
        return f"{self.group_id}:{self.artifact_id}:{self.version}"

class PyPiLibrary():
    name: str
    version: str

    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version

class PythonWheelLibrary():
    path: str

    def __init__(self, path: str):
        self.path = path

class Libraries():
    maven_libraries: list[MavenLibrary]
    pypi_libraries: list[PyPiLibrary]
    pythonwheel_libraries: list[PythonWheelLibrary]

    def __init__(self, maven_libraries: list[MavenLibrary] = [], pypi_libraries: list[PyPiLibrary] = [], pythonwheel_libraries: list[str] = []):
        self.maven_libraries: list[MavenLibrary] = maven_libraries
        self.pypi_libraries: list[PyPiLibrary] = pypi_libraries
        self.pythonwheel_libraries: list[PythonWheelLibrary] = pythonwheel_libraries

    def add_maven_library(self, maven_library: MavenLibrary):
        self.maven_libraries.append(maven_library)

    def add_pypi_library(self, pypi_library: PyPiLibrary):
        self.pypi_libraries.append(pypi_library)

    def add_pythonwhl_library(self, whl_library: PythonWheelLibrary):
        self.pythonwheel_libraries.append(whl_library)
