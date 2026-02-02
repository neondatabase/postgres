-- Test buffer pool resizing and shared memory allocation tracking
-- This test resizes the buffer pool multiple times and monitors
-- shared memory allocations related to buffer management
-- TODO: The test sets shared_buffers values in MBs. Instead it could use values
-- in kBs so that the test runs on very small machines.

-- TODO: test the actual memory allocated in the shared memory segments.

-- Create a view for buffer-related shared memory allocations
CREATE VIEW buffer_allocations AS
SELECT name, segment, size, allocated_size
FROM pg_shmem_allocations
WHERE name IN ('Buffer Blocks', 'Buffer Descriptors', 'Buffer IO Condition Variables',
               'Checkpoint BufferIds')
ORDER BY name;

-- Note: We exclude the 'main' segment even if it contains the shared buffer
-- lookup table because it contains other shared structures whose total sizes
-- may vary as the code changes.
CREATE VIEW buffer_segments AS
SELECT name, size, reserved_size
FROM pg_shmem_segments
WHERE name <> 'main'
ORDER BY name;

-- Enable pg_buffercache for buffer count verification
CREATE EXTENSION IF NOT EXISTS pg_buffercache;

-- Test 1: Default shared_buffers
SHOW shared_buffers;
SHOW max_shared_buffers;
SELECT * FROM buffer_allocations;
SELECT * FROM buffer_segments;
SELECT COUNT(*) AS buffer_count FROM pg_buffercache;
-- Calling pg_resize_shared_buffers() without changing shared_buffers should be a no-op.
SELECT pg_resize_shared_buffers();
SHOW shared_buffers;
SELECT * FROM buffer_allocations;
SELECT * FROM buffer_segments;
SELECT COUNT(*) AS buffer_count FROM pg_buffercache;

-- Test 2: Set to 64MB
ALTER SYSTEM SET shared_buffers = '64MB';
SELECT pg_reload_conf();
-- reconnect to ensure new setting is loaded
\c
SHOW shared_buffers;
SELECT pg_resize_shared_buffers();
SHOW shared_buffers;
SELECT * FROM buffer_allocations;
SELECT * FROM buffer_segments;
SELECT COUNT(*) AS buffer_count FROM pg_buffercache;

-- Test 3: Set to 256MB
ALTER SYSTEM SET shared_buffers = '256MB';
SELECT pg_reload_conf();
-- reconnect to ensure new setting is loaded
\c
SHOW shared_buffers;
SELECT pg_resize_shared_buffers();
SHOW shared_buffers;
SELECT * FROM buffer_allocations;
SELECT * FROM buffer_segments;
SELECT COUNT(*) AS buffer_count FROM pg_buffercache;

-- Test 4: Set to 100MB (non-power-of-two)
ALTER SYSTEM SET shared_buffers = '100MB';
SELECT pg_reload_conf();
-- reconnect to ensure new setting is loaded
\c
SHOW shared_buffers;
SELECT pg_resize_shared_buffers();
SHOW shared_buffers;
SELECT * FROM buffer_allocations;
SELECT * FROM buffer_segments;
SELECT COUNT(*) AS buffer_count FROM pg_buffercache;

-- Test 5: Set to minimum 128kB
ALTER SYSTEM SET shared_buffers = '128kB';
SELECT pg_reload_conf();
-- reconnect to ensure new setting is loaded
\c
SHOW shared_buffers;
SELECT pg_resize_shared_buffers();
SHOW shared_buffers;
SELECT * FROM buffer_allocations;
SELECT * FROM buffer_segments;
SELECT COUNT(*) AS buffer_count FROM pg_buffercache;

-- Test 6: Try to set shared_buffers higher than max_shared_buffers (should fail)
ALTER SYSTEM SET shared_buffers = '400MB';
SELECT pg_reload_conf();
-- reconnect to ensure new setting is loaded
\c
-- This should show the old value since the configuration was rejected
SHOW shared_buffers;
SHOW max_shared_buffers;
