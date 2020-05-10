#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#define SA struct sockaddr

int main(int argc, const char* argv[] )
{
    printf("\n\nTask Name : SINK\n");
    int sockfd, connfd;
    struct sockaddr_in servaddr, cli;

    int port;
    port = atoi(argv[2]);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("Socket creation failed...\n");
        exit(0);
    }
    else
        printf("Socket successfully created..\n");
    bzero(&servaddr, sizeof(servaddr));

    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(argv[1]);
    servaddr.sin_port = htons(port);
    bzero(&(servaddr.sin_zero),8);

    // printf("Port number : %d\n",port);
    // connect the client socket to server socket
    if (connect(sockfd, (SA*)&servaddr, sizeof(servaddr)) != 0) {
        printf("Connection with the server failed...\n");
        exit(0);
    }
    else
        printf("Connected to the server..\n");

    char buff[10000];
    bzero(buff, sizeof(buff));
    read(sockfd, buff, sizeof(buff));

    bzero(buff, sizeof(buff));
    read(sockfd, buff, sizeof(buff));

    // printf("Task output to be sent \n");
    // for(int i=0; i<strlen(buff); i++) printf("%c",buff[i]);
    // printf("\n");
    send(sockfd , buff , strlen(buff) , 0 );
    // close the socket
    close(sockfd);
}
