const os = require("os");
const fs = require("fs");
const cluster = require("cluster");
const rl = require("readline");

// Use cpu count for # of workers. Assuming master will be mostly idle while workers run
const CPU_COUNT = os.cpus().length;
const FILE_NAME = process.env.FILE_NAME || "file.txt";

const workerExitHandler = (() => {
  // When all workers exit master can exit
  let exited = 0;
  return () => {
    if (++exited === CPU_COUNT) {
      process.exit(0);
    }
  };
})();

const workerMessageHandler = (() => {
  // Messages from workers update shared score.
  const totalScore = {};
  let count = 0;
  return msg => {
    Object.entries(msg).forEach(([key, value]) => {
      if (key in totalScore) {
        totalScore[key] += value;
      } else {
        totalScore[key] = value;
      }
    });

    // Last update sorts and writes solution writes to file
    if (++count === CPU_COUNT) {
      const scoreString = Object.entries(totalScore)
        .sort(([_, val1], [__, val2]) => val2 - val1)
        .map(([key, value]) => `${key}: ${value}`)
        .join("\r\n");
      const solutionFileName = `solution-${Date.now()}.txt`;
      fs.writeFile(solutionFileName, scoreString, err => {
        if (err) {
          throw err;
        }
        console.log(`Solutions written to: ${solutionFileName}`);
      });
    }
  };
})();

if (cluster.isMaster) {
  let lines = 0;
  // Make file/streams for each cpu available
  const newFileStreams = Array.from(new Array(CPU_COUNT), (_, i) => {
    const filename = `${i}-${FILE_NAME}`;
    fs.closeSync(fs.openSync(filename, "w"));
    return new fs.createWriteStream(filename);
  });

  // Push our main file into these streams
  const lineReader = rl.createInterface({
    input: fs.createReadStream(FILE_NAME)
  });
  console.log(`Spliting file into ${CPU_COUNT} other files`);

  lineReader.on("line", line => {
    const streamIdx = lines++ % CPU_COUNT;
    newFileStreams[streamIdx].write(`${line}\r\n`);
  });

  lineReader.on("close", () => {
    console.log("Closing Streams...");
    for (const stream of newFileStreams) {
      stream.end();
    }
    console.log("Streams Closed, Starting Workers...");

    for (var i = 0; i < CPU_COUNT; i++) {
      // Spawn workers with thier individual file pieces
      const worker = cluster.fork({ FILE_NAME: `${i}-${FILE_NAME}` });
      worker.on("message", workerMessageHandler);
      worker.on("exit", workerExitHandler);
    }
  });
} else {
  // Read file line by line and keep score
  const score = {};
  let count = 0;

  const lineReader = rl.createInterface({
    input: new fs.createReadStream(FILE_NAME)
  });

  lineReader.on("line", line => {
    const hostname = new URL(line).hostname;
    // Get last two domains from hostname
    const domain = hostname
      .split(".")
      .reverse()
      .slice(0, 2)
      .reverse()
      .join(".");
    if (domain in score) {
      score[domain] = score[domain] + 1;
    } else {
      score[domain] = 1;
    }
  });

  // When done with file emit you total score to master
  lineReader.on("close", () => {
    process.send(score);
    fs.unlink(FILE_NAME, () => {
      process.exit(0);
    });
  });
}
