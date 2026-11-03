FROM node:20-alpine

WORKDIR /app
COPY index.html support.js server.js ./

EXPOSE 8080
USER node
CMD ["node", "server.js"]
