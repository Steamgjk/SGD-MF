
#ifndef RDMA_SERVER_H
#define RDMA_SERVER_H

#include "rdma_common.h"

int setup_client_resources();
int start_rdma_server(struct sockaddr_in *server_addr);
int accept_client_connection();
int send_server_metadata_to_client();
int send_server_metadata_to_client1(void* buf_to_rwrite, size_t buf_sz);
int disconnect_and_cleanup();
int rdma_server_init(char* local_ip, int local_port, void* register_buf, size_t register_sz);
#endif

