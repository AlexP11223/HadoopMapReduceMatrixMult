Hadoop MapReduce program for sparse matrix multiplication, implemented in Kotlin.

Also includes sequential (not Hadoop/MapReduce) version of matrix multiplication.

Input files are in COO/ijv format, with additional first column for matrix name/id (can be M or N).

Example for 

| | | |
| --- | --- | --- |
| 4 | 0 | 1 |
| 0 | 6 | 0 |

    M,0,0,4
    M,0,2,1
    M,1,1,6
    
`tools/` folder contains Python script for generating random sparse matrices in this format.
    
# How to build

Requirements:
- JDK 8+.
- Maven 3+.

Run Maven **package** phase. This will download all dependencies, run JUnit tests and build JAR file.

(Maven is included in popular Java IDEs such as IntelliJ Idea or Eclipse. You can run it either via your IDE Maven plugin or from command line in separate [Maven installation](https://maven.apache.org/install.html): `mvn package`.)

`target/` folder will contain JAR file.

# Usage

The easiest way is to take `*-fat.jar` from `target/`, it includes all dependencies. Then run it as usual via `yarn jar`.

