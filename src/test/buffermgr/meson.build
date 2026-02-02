# Copyright (c) 2022-2025, PostgreSQL Global Development Group

tests += {
  'name': 'buffermgr',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'buffer_resize',
    ],
    'regress_args': ['--temp-config', files('buffermgr_test.conf')],
  },
  'tap': {
    'env': {
      'enable_injection_points': get_option('injection_points') ? 'yes' : 'no',
    },
    'tests': [
      't/001_resize_buffer.pl',
      't/003_parallel_resize_buffer.pl',
      't/004_client_join_buffer_resize.pl',
    ],
  },
}
