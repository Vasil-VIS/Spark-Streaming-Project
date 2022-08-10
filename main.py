import math
import time
import numpy as np
from pyspark import RDD, SparkContext
from pyspark.streaming import DStream, StreamingContext

# Given:
window_size = 60
tau = 3000
epsilon = 0.001
delta = 0.01

d = int(np.ceil(np.log(1/delta)))
# d = 5
w = int(np.ceil(np.exp(1)/epsilon))
# w = 2719


def generate_prime(upto=100000000):
    """
    Generate a list of primes up to the given number
    """
    # Create list of all numbers from 2 to upto + 1 and skip even numbers
    all_primes = np.arange(3, upto + 1, 2)
    isprime_matrix = np.ones((upto - 1) // 2, dtype=bool)
    # Iterate through all factors of all numbers from 3 to sqrt(upto)
    for factor in all_primes[:int(math.sqrt(upto))]:
        if isprime_matrix[(factor - 2) // 2]:
            isprime_matrix[(factor * 3 - 2) // 2::factor] = 0
    # Return all primes
    return np.insert(all_primes[isprime_matrix], 0, 2)


def compute_inner_product(sketch_a, sketch_b):
    """
    Compute the inner product of two sketches with the same w,d parameters
    """
    # Get number of rows of CM sketch
    d = len(sketch_a)
    # Set initial value for the minimum dot product of each row
    min_dot_product = np.inf

    # Iterate through rows
    for row_index in range(d):
        # Estimate dot product for row
        current_dot_product = np.dot(sketch_a[row_index], sketch_b[row_index])
        # Update the current minimum dot product estimation
        min_dot_product = min(min_dot_product, current_dot_product)

    # Return minimum dot product estimate for rows
    return min_dot_product


def map_to_key_value(lines: DStream) -> DStream:
    """
    Maps incoming lines to key-value pairs (server, [IPs])
    """
    # Split on incoming lines
    entries = lines.flatMap(lambda x: x.split('\n'))
    # Split on comma to get [server, ip] pairs
    words = entries.map(lambda x: x.split(','))
    # Group by key (server) and map ip values to list for each server id
    return words.groupByKey().map(lambda x: (int(x[0]), np.array(list(x[1]), dtype=int)))


def values_to_sketch(x: np.ndarray, primes: list) -> np.ndarray:
    """
    Convert list of integer values to a sketch matrix of size d x w
    """
    # Create a CM_sketch (matrix with d rows, w cloumns) of 0s
    out = np.zeros([d, w])
    # For each row
    for row in range(d):
        # Get the counter for the current row using hash function ((a*x + b) % p) % w, where
        # a, b, and p are obtained from the primes list
        counter = np.abs(
            (primes[row][0] * x + primes[row][1]) % primes[row][2]) % w
        # Update the value of the counter for the current row of the CM sketch
        out[row, counter] = out[row, counter] + 1

    # Return CM_sketch for the server
    return out


def init_sketches(dstream: DStream, primes: list) -> DStream:
    """
    Initializes the sketches for each server ID
    """
    # Convert (server_id, [ip1, ip2, ...]) entries to (server_id, sketch_of_server_id)
    return dstream.map(lambda x: (x[0], values_to_sketch(x[1], primes)))


def join_sketches(dstream: DStream) -> DStream:
    """
    Joins the RDDs of the sketches based on given combinations
    """
    def process(rdd: RDD):
        # Cartesian join with self as long as sId1 < sId2
        joined = rdd.cartesian(rdd).filter(lambda x: x[0][0] < x[1][0])
        # Map to ((server1, server2), (sketch1, sketch2)) combinations
        return joined.map(lambda x: ((x[0][0], x[1][0]), (x[0][1], x[1][1])))

    # Process each rdd in dstream to produce ((server1, server2), (sketch1, sketch2)) combinations
    return dstream.transform(lambda rdd: process(rdd))


def print_results(rdd):
    """
    Prints the results every 2s
    """
    global nr_of_updates
    for count in rdd.collect():
        # Increment total number of updates
        nr_of_updates += count[1][1]
        # Print the total number of updates, the number of pairs with similarity >= tau, and the current time
        print(">> " + str(nr_of_updates) + ", " +
              str(count[1][0]) + ", " + str(np.ceil(time.time() - start_time)))


def run(ssc: StreamingContext, primes: list):
    # Obtain incoming entries from stream
    lines = ssc.socketTextStream('localhost', 9000)
    #lines = ssc.socketTextStream('stream-host', 9000)

    # Count the number of updates
    updates = lines.count()
    # Convert the single entry of updates to a pair in order to join it
    # with the estimated number of pairs for printing the final results
    updates = updates.map(lambda x: (0, x))

    # Map to entries from stream to (server_id, [ips]) pairs
    stream = map_to_key_value(lines)
    # Map to (server_id, [ips]) pairs to (server_id, CM_sketch_of_server_id) pairs
    initial_sketches = init_sketches(stream, primes)
    # Update and expire information using sketch merging
    # set a window size of 60 seconds with a 2s sliding interval
    sketches = initial_sketches.reduceByKeyAndWindow(
        lambda x, y: x+y, lambda x, y: x-y, window_size, 2)
    # Find all server pairs using cartesian joining:
    # convert (server_id, CM_sketch_of_server_id) pairs to
    # ((server_i, server_y), (CM_sketch_i, CM_sketch_y)) pairs
    joined = join_sketches(sketches)
    # Compute the similarity between every pair of sketches: ((server1, server2), sim)
    # keep only server pairs with similarity >= tau
    similarity = joined.map(lambda x: (x[0], compute_inner_product(
        x[1][0], x[1][1]))).filter(lambda x: x[1] >= tau)
    # Count number of pairs with similarity >= tau
    similarity_count = similarity.count()
    # Convert the single entry of similarity_count to a pair in order to join it
    # with the number of processed updates for printing the final results
    similarity_count.map(lambda x: (0, x)).join(
        updates).foreachRDD(lambda rdd: print_results(rdd))


def main():
    # Set a random seed
    np.random.seed(76)
    # Generate a list of primes for the hash functions
    all_primes = generate_prime()
    # Create d lists of 3 primes, 1 for each row in the sketch
    primes = [sorted([np.random.choice(all_primes)
                      for _ in range(3)]) for _ in range(d)]

    # Store the largest third value (c) from each list
    max_p = 0
    for list in primes:
        max_p = max(max_p, list[2])

    # Update the third value of each list to be max_p
    # Thus, each hash function of the form: ((ax + b) % p) % width_of_sketch
    # has different (a,b) pairs, where a<p and b<p, which guarantees that the
    # hash functions are pairwise independent
    for list in primes:
        list[2] = max_p

    # Set SparkContext and StreamingContext
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)
    # Set checkpoint
    ssc.checkpoint("checkpoint")

    # Process streaming logic
    run(ssc, primes)

    ssc.start()
    ssc.awaitTermination()


# Get the start time of the program
start_time = time.time()
# Global variable to update the total number of updates
nr_of_updates = 0
main()
