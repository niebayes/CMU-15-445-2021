cd /home/lzx/bustub/build

make lock_manager_test -j8
make transaction_test -j8

./test/lock_manager_test
./test/transaction_test

cd ..