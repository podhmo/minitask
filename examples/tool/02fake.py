from minitask.communication import fake


output_port = fake.create_writer_port()
input_port = fake.create_reader_port()

fake.write("hello", file=output_port)
msg = fake.read(file=input_port)
print(msg)
