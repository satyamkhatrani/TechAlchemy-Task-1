const fs = require("fs");

const CHUNK_FOLDER_NAME = "./chunks/";

const readStream = fs.createReadStream("randomNums.csv", {
  highWaterMark: 500 * 1024 * 1024,
});

const customSort = () => {
  let unprocesed = "";
  let chunkName = "";
  let i = 0;

  readStream.on("data", (chunk) => {
    const chunkString = unprocesed + chunk.toString();
    unprocesed = "";

    const chunkArray = chunkString.split(",");
    unprocesed = chunkArray.pop();
    const sortedChunkArray = chunkArray.sort((a, b) => a - b);

    chunkName = `${CHUNK_FOLDER_NAME}${sortedChunkArray[0]}_${++i}.csv`;

    fs.writeFileSync(chunkName, sortedChunkArray.join(", "));
  });

  readStream.on("end", () => {
    if (unprocesed) {
      let lastChunk = fs.readFileSync(chunkName);

      const chunkString = lastChunk.toString();

      const chunkArray = chunkString.split(",");
      chunkArray.push(unprocesed);

      const sortedChunkArray = chunkArray.sort((a, b) => a - b);

      fs.unlinkSync(chunkName);

      fs.writeFileSync(
        `${CHUNK_FOLDER_NAME}${sortedChunkArray[0]}_${i}.csv`,
        sortedChunkArray.join(", ")
      );
    }

    let hasFiles = true;

    while (hasFiles) {
      let fileNameList = [];

      fs.readdirSync(CHUNK_FOLDER_NAME).forEach((file) => {
        fileNameList.push(file);
      });

      if (fileNameList.length === 0) {
        hasFiles = false;
        break;
      }

      let smallestName = fileNameList.sort()[0];

      const [num, extension] = smallestName.split("_");

      fs.appendFileSync("sortedNumbers.csv", `${num}, `);

      let smallestData = fs.readFileSync(CHUNK_FOLDER_NAME + smallestName);

      fs.unlinkSync(CHUNK_FOLDER_NAME + smallestName);

      const smallestString = smallestData.toString();

      if (smallestString) {
        const smallestArray = smallestString.split(",");

        const slicedSmallestArray = smallestArray.slice(1);

        if (slicedSmallestArray.length !== 0) {
          fs.writeFileSync(
            `${CHUNK_FOLDER_NAME}${slicedSmallestArray[0]}_${extension}`,
            slicedSmallestArray.join(",")
          );
        }
      }
    }
  });
};

exports.customSort = customSort;
