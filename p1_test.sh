#!/bin/bash

cd /home/lzx/bustub/build

make lru_replacer_test -j8 
make buffer_pool_manager_instance_test -j8
make parallel_buffer_pool_manager_test -j8

./test/lru_replacer_test
./test/buffer_pool_manager_instance_test
./test/parallel_buffer_pool_manager_test

cd ..
