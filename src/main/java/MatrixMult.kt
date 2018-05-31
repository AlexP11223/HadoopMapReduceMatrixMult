import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.LongWritable
import java.util.HashMap

internal class Mapper : org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>() {
    public override fun map(key: LongWritable, value: Text, context: Context) {
        val conf = context.configuration
        val p = conf.get("p").toInt()
        val r = conf.get("r").toInt()

        val line = value.toString()
        val (id, row, col, cellValue) = line.split(",").dropLastWhile { it.isEmpty() }.toTypedArray()

        when (id) {
            "M" -> {
                for (k in 0 until r) {
                    context.write(Text("$row,$k"), Text("$id,$col,$cellValue"))
                }
            }
            "N" -> {
                for (i in 0 until p) {
                    context.write(Text("$i,$col"), Text("$id,$row,$cellValue"))
                }
            }
            else -> throw IllegalArgumentException(id)
        }
    }
}

internal class Reducer : org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>() {
    public override fun reduce(key: Text, values: Iterable<Text>, context: Context) {
        val conf = context.configuration
        val q = conf.get("q").toInt()

        val mapM = HashMap<Int, Float>()
        val mapN = HashMap<Int, Float>()
        for (v in values) {
            val (id, ind, cellValue) = v.toString().split(",").dropLastWhile { it.isEmpty() }.toTypedArray()
            when (id) {
                "M" -> {
                    mapM[ind.toInt()] = cellValue.toFloat()
                }
                "N" -> {
                    mapN[ind.toInt()] = cellValue.toFloat()
                }
                else -> throw IllegalArgumentException(id)
            }
        }

        val result = (0 until q).fold(0f) {
            sum, j -> sum + mapM.getOrDefault(j, 0f) * mapN.getOrDefault(j, 0f)
        }
        if (result != 0f) {
            context.write(null, Text(key.toString() + "," + java.lang.Float.toString(result)))
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
        job.inputFormatClass = TextInputFormat::class.java
        job.outputFormatClass = TextOutputFormat::class.java

        job.mapperClass = Mapper::class.java
        job.reducerClass = Reducer::class.java

        FileInputFormat.addInputPath(job, Path(args[3]))
        FileOutputFormat.setOutputPath(job, Path(args[4]))

        System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }
}
