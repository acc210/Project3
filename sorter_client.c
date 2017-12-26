#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <errno.h>
#include "sorter_client.h"
/*
***************************************************
Globals
***************************************************
*/
int sock = 0;
char* metadata;
char* host;
int port;
struct info args;
int hasO; 
char* column;

pthread_mutex_t lockCSV;
/*
***************************************************
Sorter.c
***************************************************
*/

//./sc -h ls.cs.rutgers.edu -p 12345

//protocol @@ at the end of all files ***DONE***
//after while loop write protol for end request / csv file ***DONE***
//-d is ***DONE***
//-o
//error check if they dont give us 28 columns?
//FIXME check the fixme further down

int main(int argc, char **argv){

	if(argc<7){
		fprintf(stderr, "ERROR: not enough arguements\n");
		return 0;
	}

	int hasD = 0;
	hasO = 0;
	char* currPath = "";

	if (argc == 7 || argc == 9 || argc == 11) {
		if (strcmp(argv[1], "-c") != 0) {
			fprintf(stderr, "ERROR: MISSING -c FLAG");
			exit(0);
		} if (strcmp(argv[3], "-h") != 0) {
			fprintf(stderr, "ERROR: MISSING -h FLAG");
			exit(0);
		} if (strcmp(argv[5], "-p") != 0) {
			fprintf(stderr, "ERROR: MISSING -p FLAG");
			exit(0);
		}
	}
	if (argc == 9) {
		if (strcmp(argv[7], "-d") != 0) {
			fprintf(stderr, "ERROR: MISSING -d FLAG");
			exit(0);
		}
		currPath = argv[8];
		hasD = 1;
	}

	if (argc == 11) {
		if (strcmp(argv[7], "-d") != 0) {
			fprintf(stderr, "ERROR: MISSING -d FLAG");
			exit(0);
		} if (strcmp(argv[9], "-o") != 0) {
			fprintf(stderr, "ERROR: MISSING -o FLAG");
			exit(0);
		}

		currPath = argv[8];
		char* outFile = argv[10];
		hasD = 1;
		hasO = 1;
	}

	 column = argv[2];
	 host = argv[4];
	 port = atoi(argv[6]);

	 
	const char *column_titles[28] = {"color", "director_name", "num_critic_for_reviews", "duration", "director_facebook_likes", "actor_3_facebook_likes", "actor_2_name",
		"actor_1_facebook_likes", "gross","genres","actor_1_name", "movie_title", "num_voted_users", "cast_total_facebook_likes", "actor_3_name", "facenumber_in_poster",
		"plot_keywords", "movie_imdb_link", "num_user_for_reviews", "language", "country", "content_rating", "budget", "title_year", "actor_2_facebook_likes",
		"imdb_score", "aspect_ratio", "movie_facebook_likes"};

    int iter = 0;
    int colNum = -1;
	for (iter = 0; iter < 28; iter++) {
		if (strcmp(column_titles[iter], column) == 0) {
			colNum = iter;
			break;
		}
	}


	 metadata = malloc(sizeof(char *)+ 100);
	 memset(metadata, '\0', strlen(metadata));
	 char snum[2];
	 strncpy(metadata, "+", 1); //FIXME why is this 1, should the colnum be converted to a string and be here instead?
	 sprintf(snum, "%d",colNum);
	 strcat(metadata,snum);
	 strcat(metadata,"@@");
	 //printf("[%s]\n", metadata);

	 if(!hasD)
	 {
		char buff[PATH_MAX];
		currPath = getcwd(buff, sizeof(buff));
	 }
	

	if(pthread_mutex_init(&lockCSV, NULL) != 0){
        printf("Error on lock1.\n");
    }


    direcFound((void*)currPath);



	requestFile(host, port, currPath);
	
	
    pthread_mutex_destroy(&lockCSV);


	return 0;
}

void requestFile(char* host, int port, char* path) {

	startClient(host, port);

    char buff[30] = "-";

    if(send(sock, buff, 1, 0) < 0)  
	{
		fprintf(stderr, "ERROR: Send File Unsuccesful");
	}

    memset(&buff, 0, sizeof(buff));

    
    char* cpy =  "AllFiles-sorted-"; 
	char* fName = malloc(sizeof(char*) * (255)); 
	memset(fName, '\0', sizeof(fName)); //clears the memory location, otherwise there is junk
	strcpy(fName,cpy);
	strcat(fName,column);
	strcat(fName, ".csv");
	
	
	FILE* fp;
    
    
    if(hasO){
	 char newfName[PATH_MAX];
	 strcpy(newfName, path); //FIXME not this path, make output global?
	 strcat(newfName, "/");
	 strcat(newfName, fName);
	 fp = fopen(newfName , "w");  //create the new file in the output directory 
		if(fp == NULL)
		{
			fprintf(stderr, "ERROR: File %s not found\n",fName);
			exit(1);
		}
	} else {
	char newfName[PATH_MAX];
	strcpy(newfName, path);
	strcat(newfName, "/");
	strcat(newfName, fName);
	 fp = fopen(fName, "w");   //should work for -c and -d
		if(fp == NULL)
		{
		fprintf(stderr, "ERROR: File %s not found\n",fName);
		exit(1);
		}
	}

	char hold[1024] = "";  

	memset(&hold, 0, sizeof(hold));
	ssize_t elements = 0;
	while((elements = recv(sock, hold, 1024, 0)) > 0)  
	{
//	  printf("ENTERS\n");
		int numWrite = fwrite(hold, sizeof(char), elements, fp);
		if(numWrite < elements)
	    {
			fprintf(stderr, "ERROR: failed to write file \n");
		}
		

	memset(&hold, 0, sizeof(hold));

	}

	if(elements < 0)
	{
		fprintf(stderr, "ERROR: failed to recieve contents");
		exit(1);
	}

 //   printf("File Recived Succesfully\n"); //keep for testing
	fclose(fp);
	
	free(fName);
}


void sendClient(char* path) {

	char buff[1024] = "";
	char headBuff[1024] = "";

	FILE *fp = fopen(path, "r");
	if(fp == NULL)
	{
		fprintf(stderr, "ERROR: File %s not found\n", path);
		exit(1);
	}

	memset(&buff, 0, sizeof(buff));
	size_t elements; //number of elements from stream that fread has read

	send(sock, metadata, strlen(metadata), 0);
	fread(headBuff, sizeof(char), 418, fp);  //thats the # that gets rid of all the header files. assuming it will always be 28 TEST this thoroughly
// 	printf("[%s]", headBuff);
	while((elements = fread(buff, sizeof(char), 1024, fp)) > 0)
	{
	  //printf("inside loop\r\n");
		if(send(sock, buff, elements, 0) < 0)
	    {
			fprintf(stderr, "ERROR: Send File Unsuccesful");
		    break;
	    }

	   memset(&buff, 0, sizeof(buff));

		}




	fclose(fp);
	char* endReq = "@@";                       //protocol for end of ALL files
	send(sock, endReq, strlen(endReq), 0);
	close(sock);

}

void startClient(char* hostName, int port) {

     struct sockaddr_in address;
     struct sockaddr_in serv_addr;


    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("\n Socket creation error \n");
    }

    memset(&serv_addr, '0', sizeof(serv_addr));

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    struct hostent *host = gethostbyname(hostName);

    struct in_addr **addr_list;
    addr_list = (struct in_addr**)host->h_addr_list;

    char hostname[999];
    strcpy(hostname, inet_ntoa(*addr_list[0]));
 
    // Convert IPv4 and IPv6 addresses from text to binary form
    if(inet_pton(AF_INET, hostname, &serv_addr.sin_addr)<=0)
    {
        printf("\nInvalid address/ Address not supported \n");
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("\nConnection Failed \n");
		exit(0);
    }


}


void *direcFound(void* ptr) { //pass in path and file

char *currPath = (char*)ptr;
int t = 0;

pthread_t thread[200];


DIR * d;
d = opendir(currPath);
 if(!d) {
		  fprintf(stderr, "ERROR: CANNOT OPEN DIRECTORY 3: [%s]", currPath);
		  exit(0);
	  }

  struct dirent *file;
	while ((file = readdir(d)) != NULL) { //so if theres dir w/in direc
		if(file->d_type == DT_DIR) {
            if(file->d_name[0]== '.'){ //trying to deal with hidden files
			  continue;
		    }
		    char newPath[PATH_MAX];
		   	strcpy(newPath, currPath); //copy directory over
			strcat(newPath, "/");
			strcat(newPath,file->d_name); //update the directory- our new working directory updated
			strcat(newPath, "/");
			int pathLen = strlen(newPath);
			if (pathLen >= PATH_MAX)
			{
				fprintf(stderr, "ERROR: directory path is too long");
			}
			int id= pthread_create(&thread[t], NULL,(void*)&direcFound, (void*)newPath);
			t++;

			if(id){
			 printf("pthread failed create");
			}

		} else if (strstr(file->d_name, ".csv") && !(strstr(file->d_name, "-sorted-")))
		{
				if(file->d_name[0]== '.'){ //trying to deal with hidden files
				continue;
				}

				// printf ("[T %lu] %s/%s\n", pthread_self(), name, fileName);

				pthread_mutex_lock(&lockCSV);

				args.path = currPath;
				args.file = file->d_name;

 //printf("\n\nTHE CSV [%s] HAS BEEN FOUND AND PTHREAD CREATE\n\n", file->d_name);
				pthread_mutex_unlock(&lockCSV);
      
				int id= pthread_create(&thread[t], NULL,(void*)&csvFound, (void*)&args);
				t++;

			if(id)
			{
			 printf("pthread failed create");
			}
			sleep(1);
			//pthread_exit(0);
		}
  }

    int i;
	for (i =0; i < t; i++) {
	pthread_join(thread[i], NULL);
	}


  if (closedir (d)) {
        fprintf (stderr, "ERROR: Could not close directory");
        exit(0);
    }
    return 0;
}


void *csvFound(void* ptr) {

	struct info *param =  ptr;
	char* pathName = param->path;
    char* fileName = param->file;

	char fileSpace[PATH_MAX];
	strcpy(fileSpace, pathName);
	int spaceLen = strlen(fileSpace);
	if (fileSpace[spaceLen-1]  != '/') {
	strcat(fileSpace, "/");
	}
	strcat(fileSpace, fileName);

	startClient(host, port);
	sendClient(fileSpace);
	return 0;
}