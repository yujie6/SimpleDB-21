package simpledb;

import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private class Bucket {
        int left, right, num;
        Bucket(int l, int r) {
            this.left = l;
            this.right = r;
            num = 0;
        }

        int getSize() {
            return right - left + 1;
        }
    }

    private int buckets, bucketNum, bucketWidth, total;
    public int min, max;
    private Bucket[] histogram;
    public IntHistogram(int buckets) {
        this.buckets = buckets;
        this.min = Integer.MAX_VALUE;
        this.max = Integer.MIN_VALUE;
        this.total = 0;
    }

    public void build() {
        bucketNum = Math.min(max - min + 1, buckets);
        bucketWidth = (max - min + 1) / bucketNum;
        this.histogram = new Bucket[bucketNum];
        for (int i = 0; i < bucketNum; i++) {
            if (i == bucketNum - 1) histogram[i] = new Bucket(min + bucketWidth * i, max);
            else histogram[i] = new Bucket(min + bucketWidth * i, min + bucketWidth * (i+1) - 1);
        }
    }

    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.total = 0;
        build();
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int index = Math.min((v - min) / bucketWidth, bucketNum - 1);
        this.histogram[index].num += 1;
        this.total += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // optim: v < min or v > max,
        if (v < min) {
            switch (op) {
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                case EQUALS: {
                    return 0.0;
                }
                case NOT_EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ: {
                    return 1.0;
                }
            }
        }
        if (v > max) {
            switch (op) {
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                case EQUALS: {
                    return 0.0;
                }
                case NOT_EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQ: {
                    return 1.0;
                }
            }
        }
        int index = Math.min((v - min) / bucketWidth, bucketNum - 1);
        Bucket b = histogram[index];
        double t = 0.0;
        switch (op) {
            case EQUALS: {
                t = (double)b.num / (double)(b.right - b.left + 1) / total;
                return t;
            }
            case LESS_THAN: {
                for (int i = 0; i < index; i++) {
                    t += histogram[i].num / (double) total;
                }
                t += (double) b.num * (v - b.left) / b.getSize() / (double) total;
                break;
            }
            case GREATER_THAN: {
                for (int i = bucketNum - 1; i > index; i--) {
                    t += histogram[i].num / (double) total;
                }
                t += (double) b.num * (b.right - v) / b.getSize() / (double) total;
                break;
            }
            case NOT_EQUALS: {
                t = 1.0 - b.num / (b.getSize() + 0.0) / (double) total;
                break;
            }
            case LESS_THAN_OR_EQ: {
                for (int i = 0; i < index; i++) {
                    t += histogram[i].num / (double) total;
                }
                t += (double) b.num * (v - b.left + 1) / b.getSize() / (double)total;
                break;
            }
            case GREATER_THAN_OR_EQ: {
                for (int i = bucketNum - 1; i > index; i--) {
                    t += histogram[i].num / (double)total;
                }
                t += (double) b.num * (b.right - v + 1) / b.getSize() / (double)total;
                break;
            }
        }
        return t;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder("Histogram of size: ").append(bucketNum);
        sb.append("; \n with min,max to be ").append(min).append(", ").append(max);
        sb.append("; \n width to be ").append(bucketWidth);
        sb.append("; \n total to be ").append(total).append("\n");
        for (int i = 0; i < bucketNum; i++) {
            sb.append("Bucket ").append(i).append(" has num ").append(histogram[i].num).append("\n");
        }
        return sb.toString();
    }
}
