# Specify the base Docker image. You can read more about
# the available images at https://crawlee.dev/docs/guides/docker-images
# You can also use any other image with Node.js.
FROM apify/actor-node-playwright-chrome:20

# Copy just package.json and package-lock.json
# to speed up the build using Docker layer cache.
COPY package*.json ./

# Install NPM packages, skipping optional and development dependencies to
# keep the image small.
RUN npm install --omit=dev

# Copy the rest of the application source code.
COPY . ./

# Run the image.
CMD [ "npm", "start" ]
