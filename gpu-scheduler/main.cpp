#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <thread>

#include <grpcpp/grpcpp.h>
#include "proto/gpu_status.grpc.pb.h"  // Generated from your proto file

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using cabinet::TaskStatus;
using cabinet::Reply;
using cabinet::Scheduler;

// SchedulerServiceImpl implements the Scheduler service.
class SchedulerServiceImpl final : public Scheduler::Service {
public:
    // ExecuteTask receives a TaskStatus message, simulates scheduling/execution,
    // and returns a Reply.
    Status ExecuteTask(ServerContext* context, const TaskStatus* request, Reply* reply) override {
        std::cout << "Received ExecuteTask request:" << std::endl;
        std::cout << "  TaskID: " << request->task_id() << std::endl;
        std::cout << "  Assigned Board: " << request->assigned_board() << std::endl;
        std::cout << "  GPUReq: " << request->gpu_req() << std::endl;
        std::cout << "  Deadline: " << request->deadline() << std::endl;
        std::cout << "  Submit Time: " << request->submit_time() << std::endl;
        std::cout << "  Start Time: " << request->start_time() << std::endl;

        //Simulate task execution !
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        reply->set_reply("Task executed successfully by C++ scheduler");
        std::cout << "Execution complete for TaskID: " << request->task_id() << std::endl;
        return Status::OK;
    }
};

void RunServer(const std::string& server_address) {
    SchedulerServiceImpl service;

    ServerBuilder builder;
    // Listen on the given address without any authentication.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register the service.
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "C++ Scheduler server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    // Default address is for node 0.
    std::string server_address = "127.0.0.1:11000";
    // If a command-line argument is provided, use that as the server address.
    if (argc > 1) {
        server_address = argv[1];
    }
    RunServer(server_address);
    return 0;
}
