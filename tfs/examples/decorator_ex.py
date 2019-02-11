def add_to_queue_out(func):
    def magic(*args):
        print('start new magic')
        print(args)
        print(args[0].test_number)
        func(*args)
        print('end new magic')
    return magic


class Test(object):
    test_number = 10

    def add_to_queue_in(foo):
        test_number = 11

        def magic(self, *args):
            print('function name ', foo.__name__)
            print("start magic")
            print(args)
            print(self.test_number)
            print(test_number)
            foo(self, *args)
            print("end magic")
        return magic

    @add_to_queue_in
    def bar(self, msg):
        print("normal call")
        print(msg)

    @add_to_queue_out
    def bar_new(self, msg):
        print("normal new  call")
        print(msg)


test = Test()

print('--------START------------------')
test.bar('hello')
print('--------SEPARATOR--------------')
test.bar_new('hello again')
print('--------END--------------------')
