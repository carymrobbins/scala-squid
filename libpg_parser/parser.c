#include <pg_query.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
  PgQueryParseResult result;
  char next;
  char* buffer;
  char* tmp;
  size_t buflen = 100;
  size_t bufcur = 0;

  /* Consume all input from stdin. */
  buffer = malloc(buflen + 1);
  while ((next = getchar()) != EOF) {
    if (bufcur > buflen) {
      buflen *= 2;
      tmp = realloc(buffer, buflen + 1);
      if (tmp == NULL) {
        fprintf(stderr, "Unable to realloc buffer to %zu bytes\n", buflen);
        free(buffer);
        return 1;
      }
      buffer = tmp;
    }
    buffer[bufcur] = next;
    ++bufcur;
  }

  /* Parse */
  pg_query_init();
  result = pg_query_parse(buffer);

  free(buffer);

  if (result.error) {
    fprintf(stderr, "error: %s at %d\n", result.error->message, result.error->cursorpos);
  } else {
    printf("%s\n", result.parse_tree);
  }

  pg_query_free_parse_result(result);

  if (result.error) return 1;
  return 0;
}
