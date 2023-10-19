import csv
import heapq
import os

chunk_size = 500000  # number of rows
input_file = "spotify_songs.csv"
temp_directory = "temp_chunks"

def create_chunks():
    if not os.path.exists(temp_directory):
        os.makedirs(temp_directory)

    with open(input_file, encoding="utf8") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        headers = next(csv_reader)

        chunk = []
        chunk_id = 0
        row_count = 0

        for row in csv_reader:
            for x in range(70):
                chunk.append(row)
                row_count += 1
            if row_count >= chunk_size:
                save_chunk(chunk, chunk_id, headers)
                chunk_id += 1
                chunk = []
                row_count = 0

        # save the last chunk
        if chunk:
            save_chunk(chunk, chunk_id, headers)


def save_chunk(chunk, chunk_id, headers):
    chunk.sort(key=lambda x: x[0])  # sort by the first column
    with open(f"{temp_directory}/chunk_{chunk_id}.csv", "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(chunk)


def merge_chunks():
    sorted_file = "sorted_spotify_songs.csv"
    chunk_files = [open(f"{temp_directory}/chunk_{i}.csv", 'r', encoding='utf-8') for i in range(len(os.listdir(temp_directory)))]

    csv_readers = [csv.reader(f, delimiter=',') for f in chunk_files]
    queue = []

    # skip headers and initialize the priority queue
    for index, reader in enumerate(csv_readers):
        next(reader)
        row = next(reader, None)
        if row:
            heapq.heappush(queue, (row, index))

    with open(sorted_file, "w", newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        headers = next(csv.reader(open(input_file, 'r', encoding='utf-8')))
        writer.writerow(headers)

        while queue:
            row, index = heapq.heappop(queue)
            writer.writerow(row)

            row = next(csv_readers[index], None)
            if row:
                heapq.heappush(queue, (row, index))

    for f in chunk_files:
        f.close()


if __name__ == '__main__':
    create_chunks()
    merge_chunks()

