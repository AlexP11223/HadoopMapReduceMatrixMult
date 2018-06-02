import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.io.WritableComparator


class IntIntPairWritable(var first: Int, var second: Int) : WritableComparable<IntIntPairWritable> {
    @Suppress("unused")
    constructor() : this(0, 0)

    override fun write(out: DataOutput?) {
        out!!.writeInt(first)
        out.writeInt(second)
    }

    override fun readFields(input: DataInput?) {
        first = input!!.readInt()
        second = input.readInt()
    }

    override fun compareTo(other: IntIntPairWritable?): Int {
        val firstCmp = first.compareTo(other!!.first)
        return if (firstCmp == 0) second.compareTo(other.second) else firstCmp
    }

    override fun toString() = "IntIntPairWritable(first=$first, second=$second)"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as IntIntPairWritable

        if (first != other.first) return false
        if (second != other.second) return false

        return true
    }

    override fun hashCode(): Int {
        var result = first
        result = 31 * result + second
        return result
    }
}

// https://vangjee.wordpress.com/2012/03/30/implementing-rawcomparator-will-speed-up-your-hadoop-mapreduce-mr-jobs-2/
class IntIntPairComparator private constructor() : WritableComparator(IntIntPairWritable::class.java) {
    override fun compare(b1: ByteArray, s1: Int, l1: Int, b2: ByteArray, s2: Int, l2: Int): Int {
        val i1 = WritableComparator.readInt(b1, s1)
        val i2 = WritableComparator.readInt(b2, s2)

        val firstCmp = i1.compareTo(i2)
        if (firstCmp != 0)
            return firstCmp

        val j1 = WritableComparator.readInt(b1, s1 + 4)
        val j2 = WritableComparator.readInt(b2, s2 + 4)

        return j1.compareTo(j2)
    }
}