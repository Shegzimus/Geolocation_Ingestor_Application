# import threading
# MULTITHREADING LOGIC START

    # dfs_stack = deque(high_density_stack)


    # # Shared state for workers
    # processed = set()
    # seen_place_ids = set()
    # chunk_buffer = []
    # deep_count = 0


# def worker():
#         state_lock = threading.Lock()
#         nonlocal deep_count
#         while True:
#             try:
#                 # Thread‐safe atomic pop
#                 lat, lng, size = dfs_stack.pop()
#             except IndexError:
#                 # stack empty → we’re done
#                 return

#             key = f"{lat:.6f},{lng:.6f},{size:.6f}"
#             # protect processed‐set and deep_count
#             with state_lock:
#                 if size <= MIN_STEP or key in processed:
#                     continue
#                 processed.add(key)
#                 deep_count += 1

#             # 1) call the API
#             places, count, pages = get_nearby_places(
#                 lat,
#                 lng,
#                 int(INITIAL_RADIUS * (size / INITIAL_STEP)),
#                 TYPE
#             )

#             # 2) record new places
#             new = []
#             with state_lock:
#                 for p in places:
#                     pid = p["place_id"]
#                     if pid not in seen_place_ids:
#                         seen_place_ids.add(pid)
#                         chunk_buffer.append(p)
#                         new.append(p)

#             # 3) chunk‐write if full
#             if new and len(chunk_buffer) >= CHUNK_SIZE:
#                 with state_lock:
#                     flush_chunk(city, chunk_buffer)

#             # 4) only subdivide if truly capped
#             if pages == MAX_PAGES and count == MAX_RESULTS_PER_PAGE * MAX_PAGES:
#                 for sub in subdivide_tile((lat, lng, size)):
#                     # atomic append on deque
#                     dfs_stack.append(sub)

#             # 5) checkpoint every so often
#             if deep_count % 10 == 0:
#                 with state_lock:
#                     save_checkpoint({
#                         "high_density_stack": list(dfs_stack),
#                         "processed": processed,
#                         "seen_place_ids": seen_place_ids,
#                         "deep_count": deep_count
#                     })




# def spawn_worker():
#     threads = []
#     for i in range(MAX_WORKERS):
#         t = threading.Thread(target=worker, name=f"dfs-worker-{i}")
#         t.start()
#         threads.append(t)

#     # wait for everyone to finish
#     for t in threads:
#         t.join()
