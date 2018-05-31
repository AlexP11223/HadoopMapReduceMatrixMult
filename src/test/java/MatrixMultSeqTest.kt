import org.junit.Assert.*
import org.junit.*
import org.junit.rules.TemporaryFolder
import java.io.File

class MatrixMultSeqTest {
    @Rule
    @JvmField
    val tmpFolder = TemporaryFolder()

    @Test
    fun multiplySparseMatrices() {
        val inputFile1Path = tmpFolder.root.absolutePath + "/m.txt"
        val inputFile2Path = tmpFolder.root.absolutePath + "/n.txt"
        val outputFilePath = tmpFolder.root.absolutePath + "/out.txt"

        File(inputFile1Path).printWriter().use { out ->
            out.print("""
                M,0,0,4
                M,0,2,1
                M,1,1,6""".trimIndent())
        }

        File(inputFile2Path).printWriter().use { out ->
            out.print("""
                N,0,0,2
                N,0,2,5
                N,1,1,1
                N,1,3,1
                N,2,0,5
                N,2,2,7""".trimIndent())
        }

        multiplySparseMatrices(2, 3, 4, inputFile1Path, inputFile2Path, outputFilePath)

        val result = File(outputFilePath).readText().trim()

        assertEquals("""
            0,0,13.0
            0,2,27.0
            1,1,6.0
            1,3,6.0""".trimIndent(), result)
    }
}