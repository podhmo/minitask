from minitask.network import fake


output_port = fake.create_writer_port()
input_port = fake.create_reader_port()

fake.write("hello", port=output_port)
msg = fake.read(port=input_port)
print(msg)
