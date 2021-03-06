#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#define SA struct sockaddr

#define TINY_COEF    1.0E-20

int main(int argc, const char* argv[] ){
  printf("\nTask Name : MATRIX\n");
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
  const int len = atoi(buff);
  // printf("Length received : %d \n",len);
  int nCount;

  bzero(buff, sizeof(buff));
  read(sockfd, buff, sizeof(buff));
  // printf("Input received from slave : \n");
  // for(int i=0; i<strlen(buff);i++) printf("%c",buff[i]);
  // printf("\n\n");

  int temparr[len];
  for(int i=0; i<len; i++) temparr[i]=0;

  int length = strlen(buff);
  int negative = 0,j=0;
  for(int i=0; i<length; i++){
    if (buff[i] == ',') {
          if(negative==1){
            temparr[j]=-1*temparr[j];
            negative = 0;
          }
          j++;
    }
    else if(buff[i] == '-'){
        negative = 1;
        continue;
    }
    else {
         temparr[j] = temparr[j] * 10 + (buff[i] - 48);
        if(i == length-1 && negative==1) temparr[j]=-1*temparr[j];
    }
  }

  double d = (double)len;
  if(sqrt(d)==floor(sqrt(d))){
    nCount = (int)sqrt(d);
  }
  else{
    double l1 = (double)(len-1);
    while(1){
      printf("%f \n",l1);
      if(sqrt(l1)==floor(sqrt(l1))) {
        nCount = (int)sqrt(l1);
        break;
      }
      else l1--;
    }
  }

  float **matrixA;    /* The input matrix for LU decomposition */
  float *resultB ;     /* The result vector -- solution for 'X' */
  float *rowPtr;
  matrixA = (float**)malloc( ( nCount + 1 ) * sizeof( float* ) ) ;
  resultB = (float*)malloc( ( nCount + 1 ) * sizeof( float ) ) ;
  rowPtr = (float*)malloc( nCount * ( nCount + 1 ) * sizeof( float ) ) ;

  /* Gets 'matrixA' and 'nCount' values from test data*/
  for(int row = 0 ; row < nCount ; row++ ){
      matrixA[row] = rowPtr ;
      rowPtr += nCount ;
  }

  int count=0;
  for(int i=0; i<nCount; i++){
    for(int j=0; j<nCount; j++) {
      matrixA[i][j]=(float)temparr[count];
      count++;
    }
    resultB[i] = (float)(temparr[count]+1);
  }

  static int row1 ;
  static int col1 ;
  static int k1 ;
  static float bigElmnt1 ;
  static int maxRow1 ;
  static float element1 ;
  static float sum1 ;
  static float determinant1 ;
  static float *scaleVector ;
  static int ii1 ;

  /* First, perform LU decomposition on the input matrix 'A' */
  maxRow1 = 0 ;

  /* Allocate for the scaling vector */
  scaleVector = (float *)malloc( sizeof( float ) * nCount ) ;

  /* Really just the sign of the determinant( +/-1 )*/
  determinant1 = 1.0 ;

  /* Determine the implicit scaling of the input matrix */
  for( row1 = 0 ; row1 < nCount ; row1++ ){
      bigElmnt1 = 0.0 ;       /* ...by finding the largest element */
      for( col1 = 0 ; col1 < nCount ; col1++ ){
          element1 = fabs( matrixA[row1][col1] ) ;
          if( element1 > bigElmnt1 ) bigElmnt1 = element1 ;
      }
      scaleVector[row1] = 1.0 / bigElmnt1 ;
  } /* End of 'for' to determine scaling */

  /* This is Crout's method */
  for( col1 = 0 ; col1 < nCount ; col1++ ) {
      for( row1 = 0 ; row1 < col1 ; row1++ ) {    /* First, find the upper triangle */
          sum1 = matrixA[row1][col1] ;
          for( k1 = 0 ; k1 < row1 ; k1++ )  sum1 -= matrixA[row1][k1] * matrixA[k1][col1] ;
          matrixA[row1][col1] = sum1 ;
      }

      bigElmnt1 = 0.0 ;   /* Next, search for the largest */
      for( row1 = col1 ; row1 < nCount ; row1++ ) {     /* ...pivot element */
          sum1 = matrixA[row1][col1] ;
          for( k1 = 0 ; k1 < col1 ; k1++ ) sum1 -= matrixA[row1][k1] * matrixA[k1][col1] ;
          matrixA[row1][col1] = sum1 ;
          element1 = scaleVector[row1] * fabs( sum1 ) ;

          if( element1 >= bigElmnt1 ) { /* Is this the best pivot element ? */
              bigElmnt1 = element1 ;
              maxRow1 = row1 ;
          }
      }

      if( col1 != maxRow1 ) {                        /* Need to interchange rows ? */
          for( k1 = 0 ; k1 < nCount ; k1++ ) {       /* Yes, interchange the rows... */
              element1 = matrixA[maxRow1][k1] ;
              matrixA[maxRow1][k1] = matrixA[col1][k1] ;
              matrixA[col1][k1] = element1 ;
          }

          element1 = resultB[maxRow1] ;               /* Including the result vector */
          resultB[maxRow1] = resultB[col1] ;
          resultB[col1] = element1 ;
          element1 = scaleVector[maxRow1] ;
          scaleVector[maxRow1] = scaleVector[col1] ;
          scaleVector[col1] = element1 ;

          determinant1 = -determinant1 ;              /* Will flip the sign of the determinant */
      }

      if( matrixA[col1][col1] == 0.0 ) matrixA[col1][col1] = TINY_COEF ;

      if( col1 != ( nCount - 1 ) ) {
          element1 = 1.0 / matrixA[col1][col1] ;
          for( row1 = ( col1 + 1 ) ; row1 < nCount ; row1++ ) matrixA[row1][col1] *= element1 ;
      }
  } /* end for() Crout's method */

  /* Now, perform forward/backward substitution on the decomposition of matrix 'A' */
  ii1 = -1 ;

  for( row1 = 0 ; row1 < nCount ; row1++ ) {                    /* Do the forward substitution */
      sum1 = resultB[row1] ;
      if( ii1 != -1 ){
          for( col1 = ii1 ; col1 <= ( row1 - 1 ) ; col1++ ) sum1 -= matrixA[row1][col1] * resultB[col1] ;
      }
      else{
          if( sum1 != 0.0 ) ii1 = row1 ;
      }
      resultB[row1] = sum1 ;
  } /* end of forward substitution */


  for( row1 = ( nCount - 1 ) ; row1 >= 0 ; row1-- ) {             /* Now do the back-substitution */
      sum1 = resultB[row1] ;
      if( row1 != ( nCount - 1 ) ){
          for( col1 = ( row1 + 1 ) ; col1 < nCount ; col1++ )  sum1 -= matrixA[row1][col1] * resultB[col1] ;
      }
      resultB[row1] = sum1 / matrixA[row1][row1] ;                /* Store element of solution vector X */
  }
  /* End of function 'FwdBackSubst' */

  // for( row1 = 0 ; row1 < nCount ; row1++ ) printf("%f ", resultB[row1] ) ;
  // printf("\n");
  /* Next, calculate the determinant of matrix 'A' */
  // for( col1 = 0 ; col1 < nCount ; col1++ )  determinant1 *= matrixA[col1][col1] ;
  // printf("%f \n",determinant1);
  char result[10000];
  bzero(result, sizeof(result));
  count = 0;
  for(int i=0; i<nCount; i++){
    char temp[20];
    sprintf(temp, "%d", (int)resultB[i]);
    for(int j=0; j<strlen(temp); j++){
      result[count] = temp[j];
      count++;
    }
    if(i<nCount-1){
      result[count]=',';
      count++;
    }
  }

  // printf("Task output to be sent \n");
  // for(int i=0; i<strlen(result); i++) printf("%c",result[i]);
  // printf("\n");
  send(sockfd , result , strlen(result) , 0 );
  // close the socket
  close(sockfd);
}
