import threading
import string

input_file = str()
with open('input.txt', 'r') as file:
    input_file = file.read()
for i in input_file:
    if i in string.punctuation:
        input_file = input_file.replace(i, "")

def map_function(input_str):
    words = input_str.split()
    return [(word, 1) for word in words]

def reduce_function(key, values):
    return (key, sum(values))

def shuffle_function(mapped_values):
    shuffled = {}
    for k, v in mapped_values:
        if k not in shuffled:
            shuffled[k] = []
        shuffled[k].append(v)
    return shuffled.items()

def map_reduce(input_data, num_threads):
    chunk_size = len(input_data) // num_threads
    chunks = [input_data[i : i + chunk_size] for i in range(0, len(input_data), chunk_size)]
    threads = []
    for chunk in chunks:
        thread = threading.Thread(target=map_reduce_worker, args=(chunk,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def map_reduce_worker(input_data):
    mapped_values = map_function(input_data)
    shuffled_values = shuffle_function(mapped_values)
    reduced_values = [reduce_function(key, values) for key, values in shuffled_values]
    for i in reduced_values:
        print(f'{i[0]} : {i[1]}')
        with open('output.txt', 'a') as file:
            file.write(f'{i[0]} : {i[1]}\n')

map_reduce(input_file, num_threads=1)