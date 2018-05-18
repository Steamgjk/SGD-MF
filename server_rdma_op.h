
#ifndef RDMA_SERVER_H
#define RDMA_SERVER_H

#include "rdma_common.h"

int setup_client_resources();
int start_rdma_server(struct sockaddr_in *server_addr);
int accept_client_connection();
int send_server_metadata_to_client();
int disconnect_and_cleanup();

#endif

