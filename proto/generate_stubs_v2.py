import os
import sys


def generate_stubs():
    proto_dir = "proto"
    output_dir = "app/infra/grpc"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        with open(os.path.join(output_dir, "__init__.py"), "w") as f:
            pass
        print(
            "gRPC stub generation disabled: data-connector is configured to use Kafka-only communication."
        )
        sys.exit(0)


if __name__ == "__main__":
    generate_stubs()
