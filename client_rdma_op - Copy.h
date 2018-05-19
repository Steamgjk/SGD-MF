
#ifndef RDMA_CLIENT_H
#define RDMA_CLIENT_H
#include "rdma_common.h"
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>
int check_src_dst();
int client_prepare_connection(struct sockaddr_in *s_addr);
int client_pre_post_recv_buffer();
int client_connect_to_server();
int client_send_metadata_to_server();
int client_send_metadata_to_server1(void* send_buf, size_t send_sz);
int start_remote_write(size_t len, size_t offset);
int client_remote_memory_ops();
int client_disconnect_and_clean();

#endif
