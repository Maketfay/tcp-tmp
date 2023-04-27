#include <sys/socket.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sqlite3.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>

#define MAX_CLIENTS 10
#define BUF_SIZE 1024  

sqlite3 *db;
char *zErrMsg = 0;
int rc;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

#pragma region Wrapper

int Socket(int domain, int type, int protocol)
{
    int res = socket(domain, type, protocol);
    if(res == -1)
    {
        perror("socket failed\n");
        exit(EXIT_FAILURE);
    }
    return res;
}

void Bind(int sockfd, const struct sockaddr *addr, socklen_t addrlen)
{
    if(bind(sockfd, addr, addrlen) < 0)
    {
        perror("bind failed\n");
        exit(EXIT_FAILURE);
    }
}

void Listen(int sockfd, int backlog)
{
    if(listen(sockfd, backlog) < 0)
    {
        perror("listen failed\n");
        exit(EXIT_FAILURE);
    }
}

int Accept(int sockfd, struct sockaddr *addr, socklen_t *addrlen)
{
    int res = accept(sockfd, addr, addrlen);
    if(res < 0)
    {
        perror("accept failed\n");
        exit(EXIT_FAILURE);
    }

    return res;
}

#pragma endregion

void handle_sigint(int sig)
{
    printf("Shutting down server...\n");
    sqlite3_close(db);
    exit(EXIT_SUCCESS);
}

void init()
{
    rc = sqlite3_open("users.db", &db);
    if (rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        exit(EXIT_FAILURE);
    }

    char *sql = "CREATE TABLE IF NOT EXISTS user (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);";
    rc = sqlite3_exec(db, sql, NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) 
    {
        perror("Can't create table: user\n");
        exit(EXIT_FAILURE);
    }

    signal(SIGINT, handle_sigint);
}

void send_message(int client_sock, char *message)
{
    if (write(client_sock, message, strlen(message)) < 0) {
        perror("Error sending message");
        exit(EXIT_FAILURE);
    }
}

int callback_user(void *data, int argc, char **argv, char **azColName)
{
    int i;
    for (i = 0; i < argc; i++) {
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    if(argc < 3)
    {
        fprintf(stderr, "Error in table user");
        exit(EXIT_FAILURE);
    }
    char message[BUF_SIZE];
    int *client_socket = (int*)data;
    snprintf(message, sizeof message, "%s %s %s", argv[0], argv[1], argv[2]);
    send_message(*client_socket, message);
    printf("\n");
    return 0;
}

void add_user(char *name, int age)
{
    char *sql;
    sql = malloc(BUF_SIZE * sizeof(char));
    snprintf(sql, BUF_SIZE, "INSERT INTO user (name, age) VALUES ('%s', %d)", name, age);

    pthread_mutex_lock(&mutex); // блокировка мьютекса
    rc = sqlite3_exec(db, sql, NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        printf("SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    pthread_mutex_unlock(&mutex); // разблокировка мьютекса

    free(sql);
}

void list_users(int client_sock)
{
    char *sql = "SELECT * FROM user";

    pthread_mutex_lock(&mutex); // блокировка мьютекса
    rc = sqlite3_exec(db, sql, callback_user, (void *)&client_sock, &zErrMsg);
    if (rc != SQLITE_OK) {
        printf("SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    pthread_mutex_unlock(&mutex); // разблокировка мьютекса
}

void *handle_client_thread(void *arg)
{
    int client_sock = *(int *)arg;
    char buf[BUF_SIZE];
    char name[BUF_SIZE];
    int age;

    while (1) {
        memset(buf, 0, BUF_SIZE);
        int bytes_read = read(client_sock, buf, BUF_SIZE);
        if (bytes_read < 0) {
            perror("Error reading from socket");
            exit(EXIT_FAILURE);
        }
        if (bytes_read == 0) {
            break;
        }
        if (strncmp(buf, "add_user", strlen("add_user")) == 0) {
            sscanf(buf, "add_user %s %d", name, &age);
            add_user(name, age);
            write(client_sock, "User added\n", strlen("User added\n"));
        } else if (strncmp(buf, "list_users", strlen("list_users")) == 0) {
            list_users(client_sock);
        } else {
            write(client_sock, "Unknown command\n", strlen("Unknown command\n"));
        }
    }
    close(client_sock);
    pthread_exit(NULL);
}



int main()
{
    init();
    int server = Socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in adr = {0};
    adr.sin_family = AF_INET;
    adr.sin_port = htons(80);
    Bind(server, (struct sockaddr *) &adr, sizeof adr);
    Listen(server, MAX_CLIENTS);

    printf("Server is running and listening for connections...\n");

    pthread_t threads[MAX_CLIENTS];
    int thread_count = 0;

    while (1) {
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    int client_sock = Accept(server, (struct sockaddr *)&client_addr, &client_addr_len);

    printf("Accepted connection from %s:%d\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

    if (thread_count < MAX_CLIENTS) {
        pthread_create(&threads[thread_count], NULL, handle_client_thread, &client_sock);
        thread_count++;
    } else {
        write(client_sock, "Server is busy, try again later.\n", strlen("Server is busy, try again later.\n"));
        close(client_sock);
    }

}

    return 0;
}