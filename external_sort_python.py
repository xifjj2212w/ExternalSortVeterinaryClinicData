import csv
import os
import time
import heapq

# Класс-обертка для изменения направления сортировки в heapq (max-heap)
class ReverseKey:
    def __init__(self, obj):
        self.obj = obj
    def __lt__(self, other):
        return self.obj > other.obj

def sort_python(input_file, output_file, key_column, order="asc"):
    start_total = time.time()
    
    # Этап 1: Разбиение (Split & Sort)
    start_split = time.time()
    temp_files = []
    chunk_size = 100000  # ~15-20 МБ в оперативной памяти ( < 10% от 1.1 ГБ)
    
    is_numeric = key_column in ['record_id', 'pet_id']
    reverse_sort = (order == "desc")

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        key_idx = header.index(key_column)
        
        chunk_idx = 0
        chunk = []
        
        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                # Числовая сортировка: преобразуем ключ в int
                if is_numeric:
                    chunk.sort(key=lambda x: int(x[key_idx]), reverse=reverse_sort)
                else:
                    chunk.sort(key=lambda x: x[key_idx], reverse=reverse_sort)
                
                temp_name = f"temp_py_{chunk_idx}.csv"
                with open(temp_name, 'w', newline='', encoding='utf-8') as tf:
                    writer = csv.writer(tf)
                    writer.writerows(chunk)
                temp_files.append(temp_name)
                chunk_idx += 1
                chunk.clear()
                
        if chunk:
            if is_numeric:
                chunk.sort(key=lambda x: int(x[key_idx]), reverse=reverse_sort)
            else:
                chunk.sort(key=lambda x: x[key_idx], reverse=reverse_sort)
            temp_name = f"temp_py_{chunk_idx}.csv"
            with open(temp_name, 'w', newline='', encoding='utf-8') as tf:
                writer = csv.writer(tf)
                writer.writerows(chunk)
            temp_files.append(temp_name)

    split_time = time.time() - start_split

    # Этап 2: Слияние (Merge)
    start_merge = time.time()
    file_handles = [open(tf, 'r', encoding='utf-8') for tf in temp_files]
    readers = [csv.reader(fh) for fh in file_handles]
    
    heap = []
    for i, reader in enumerate(readers):
        try:
            row = next(reader)
            val = int(row[key_idx]) if is_numeric else row[key_idx]
            # Для убывания оборачиваем значение в ReverseKey
            heap_val = ReverseKey(val) if reverse_sort else val
            heapq.heappush(heap, (heap_val, i, row))
        except StopIteration:
            pass

    with open(output_file, 'w', encoding='utf-8') as out_f:
        out_f.write(','.join(header) + '\n')
        while heap:
            val, idx, row = heapq.heappop(heap)
            out_f.write(','.join(row) + '\n')
            try:
                next_row = next(readers[idx])
                next_val = int(next_row[key_idx]) if is_numeric else next_row[key_idx]
                next_heap_val = ReverseKey(next_val) if reverse_sort else next_val
                heapq.heappush(heap, (next_heap_val, idx, next_row))
            except StopIteration:
                pass

    for fh in file_handles:
        fh.close()
        
    for tf in temp_files:
        os.remove(tf)

    merge_time = time.time() - start_merge
    total_time = time.time() - start_total
    
    return split_time, merge_time, total_time