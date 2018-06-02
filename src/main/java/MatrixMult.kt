import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import java.util.HashMap

internal enum class MatrixId(val value: Byte) {
    UNKNOWN(0),
    M(1),
    N(2);

    companion object {
        private val map = MatrixId.values().associateBy(MatrixId::value)
        fun fromInt(type: Byte) = map[type]!!
    }
}

internal class MapValue(var id: MatrixId, var index: Int, var value: Float) : Writable {
    @Suppress("unused")
    constructor() : this(MatrixId.UNKNOWN, 0, 0f)

    override fun write(out: DataOutput?) {
        out!!.writeByte(id.value.toInt())
        out.writeInt(index)
        out.writeFloat(value)
    }

    override fun readFields(input: DataInput?) {
        id = MatrixId.fromInt(input!!.readByte())
        index = input.readInt()
        value = input.readFloat()
    }

    override fun toString() = "MapValue(id=$id, index=$index, value=$value)"
}

internal class Mapper : org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntIntPairWritable, MapValue>() {
    public override fun map(key: LongWritable, value: Text, context: Context) {
        val conf = context.configuration
        val p = conf.get("p").toInt()
        val r = conf.get("r").toInt()

        val line = value.toString()
        val (id, rowStr, colStr, cellValueStr) = line.split(",")

        val row = rowStr.toInt()
        val col = colStr.toInt()
        val cellValue = cellValueStr.toFloat()

        when (id) {
            "M" -> {
                for (k in 0 until r) {
                    context.write(IntIntPairWritable(row, k), MapValue(MatrixId.M, col, cellValue))
                }
            }
            "N" -> {
                for (i in 0 until p) {
                    context.write(IntIntPairWritable(i, col), MapValue(MatrixId.N, row, cellValue))
                }
            }
            else -> throw IllegalArgumentException(id)
        }
    }
}

internal class Reducer : org.apache.hadoop.mapreduce.Reducer<IntIntPairWritable, MapValue, Text, Text>() {
    public override fun reduce(key: IntIntPairWritable, values: Iterable<MapValue>, context: Context) {
        val conf = context.configuration
        val q = conf.get("q").toInt()

        val mapM = HashMap<Int, Float>()
        val mapN = HashMap<Int, Float>()
        for (v in values) {
            when (v.id) {
                MatrixId.M -> {
                    mapM[v.index] = v.value
                }
                MatrixId.N -> {
                    mapN[v.index] = v.value
                }
                else -> throw IllegalArgumentException(v.id.toString())
            }
        }

        val result = (0 until q).fold(0f) {
            sum, j -> sum + mapM.getOrDefault(j, 0f) * mapN.getOrDefault(j, 0f)
        }
        if (result != 0f) {
            context.write(null, Text("${key.first},${key.second},$result"))
        }
    }
}

object MatrixMult {
    private fun showUsage() {
        System.err.println("""
            Multiplication of M (p*q) and N (q*r) matrices.
            Usage:
                MatrixMult p q r in_dir out_dir""".trimIndent())
    }

    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 5) {
            showUsage()
            System.exit(1)
        }

        val conf = Configuration()
        conf.set("p", args[0])
        conf.set("q", args[1])
        conf.set("r", args[2])

        val job = Job.getInstance(conf, "MatrixMult")
        job.setJarByClass(MatrixMult::class.java)

        job.outputKeyClass = Text::class.java
        job.outputValueClass = Text::class.java
        job.mapOutputKeyClass = IntIntPairWritable::class.java
        job.mapOutputValueClass = MapValue::class.java
        job.setSortComparatorClass(IntIntPairComparator::class.java)

        job.inputFormatClass = TextInputFormat::class.java
        job.outputFormatClass = TextOutputFormat::class.java

        job.mapperClass = Mapper::class.java
        job.reducerClass = Reducer::class.java

        FileInputFormat.addInputPath(job, Path(args[3]))
        FileOutputFormat.setOutputPath(job, Path(args[4]))

        System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }
}
