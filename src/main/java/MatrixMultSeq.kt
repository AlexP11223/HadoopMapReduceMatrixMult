import org.apache.commons.lang.time.StopWatch
import java.io.File

/**
 * Sequential (without MapReduce) multiplication of M (p*q) and N (q*r) matrices
 */
fun multiplySparseMatrices(p: Int, q: Int, r: Int, filePath1: String, filePath2: String, outputFilePath: String) {
    val sw = StopWatch()
    sw.start()

    val matrix1 = loadMatrix(filePath1, p, q)
    val matrix2 = loadMatrix(filePath2, q, r)

    val loadTime = sw.time
    println("Load time $loadTime ms")
    sw.reset()

    val result = multiplyMatrices(matrix1, matrix2, p, q, r)

    val multTime = sw.time
    println("Multiplication time $multTime ms")
    sw.reset()

    saveMatrix(outputFilePath, result, p, r)

    val saveTime = sw.time
    println("Save time $saveTime ms")
    val totalTime = loadTime + multTime + saveTime
    println("Total time $totalTime ms")
}

private fun loadMatrix(filePath: String, rowCount: Int, columnCount: Int) : Array<FloatArray> {
    val bufferedReader = File(filePath).bufferedReader()
    val matrix = Array(rowCount) { FloatArray(columnCount) }

    bufferedReader.useLines { lines -> lines.forEach { parse(it, matrix) } }

    return matrix
}

private fun saveMatrix(filePath: String, matrix: Array<FloatArray>, rowCount: Int, columnCount: Int) {
    File(filePath).bufferedWriter().use { out ->
        for (i in 0 until rowCount) {
            for (j in 0 until columnCount) {
                if (matrix[i][j] != 0f) {
                    out.append("$i,$j,${matrix[i][j]}\n")
                }
            }
        }
    }
}

private fun parse(line: String, matrix: Array<FloatArray>) {
    val (_, row, col, cellValue) = line.split(",").dropLastWhile { it.isEmpty() }.toTypedArray()
    matrix[row.toInt()][col.toInt()] = cellValue.toFloat()
}


private fun multiplyMatrices(firstMatrix: Array<FloatArray>, secondMatrix: Array<FloatArray>, r1: Int, c1: Int, c2: Int): Array<FloatArray> {
    val product = Array(r1) { FloatArray(c2) }
    for (i in 0 until r1) {
        for (j in 0 until c2) {
            for (k in 0 until c1) {
                product[i][j] += firstMatrix[i][k] * secondMatrix[k][j]
            }
        }
    }

    return product
}

object MatrixMultSeq {
    private fun showUsage() {
        System.err.println("""
            Sequential (without MapReduce) multiplication of M (p*q) and N (q*r) matrices.
            Usage:
                MatrixMultSeq p q r in_file1 in_file2 out_file""".trimIndent())
    }

    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 6) {
            showUsage()
            System.exit(1)
        }

        multiplySparseMatrices(args[0].toInt(), args[1].toInt(), args[2].toInt(), args[3], args[4], args[5])
    }
}
