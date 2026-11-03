FROM nginx:1.27-alpine

COPY index.html /usr/share/nginx/html/index.html
COPY support.js /usr/share/nginx/html/support.js

EXPOSE 80
