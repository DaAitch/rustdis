const os = require('os');
const execa = require('execa');
const { rustLogger, logger } = require('./logger');
const chalk = require('chalk');

const ROOT_DIR = `${__dirname}/..`;
const ITERATIONS = process.env.RUSTDISBENCH_SHORT === 'true' ? 1 : 5;

// redis-benchmark -t set,get -n 1000000 -r 1000000 -c 1000
const REDIS_BENCHMARK_TEST = 'set,get';
const REDIS_BENCHMARK_REQUESTS = 1000000;
const REDIS_BENCHMARK_KEYSPACELEN = 1000000;
const REDIS_BENCHMARK_CLIENTS = 1000;
const REDIS_BENCHMARK_CMD = ['redis-benchmark',
  '-t', `${REDIS_BENCHMARK_TEST}`,
  '-n', `${REDIS_BENCHMARK_REQUESTS}`,
  '-r', `${REDIS_BENCHMARK_KEYSPACELEN}`,
  '-c', `${REDIS_BENCHMARK_CLIENTS}`,
  '--csv'
];

// TODO: should we also test different `-d` values?

(async () => {
  await setup();

  let totalDurationMillis = 0;

  for (let i = 0; i < ITERATIONS; i++) {
    let n = i + 1;
    console.log(''); // new line
    logger.info(`🧪 ================= ${n}. benchmark =================`)

    let duration = await test();
    totalDurationMillis += duration;

    logger.info(`📊 took ${important(`${(duration / 1000).toFixed(1)}sec`)}`);
  }

  const models = [...new Set(os.cpus().map(cpu => cpu.model)).values()];
  const arch = os.arch();
  
  const avgDurationStr = (totalDurationMillis / 1000 / ITERATIONS).toFixed(1);
  const totalDurationStr = (totalDurationMillis / 1000).toFixed(1);

  // TODO: make csv export and present rates

  // no ora logger => should be copyable without 'i' symbol
  console.log('');
  console.log('BENCHMARK REPORT rustdis');
  console.log('');
  console.log(`benchmark:`);
  console.log(`- command:                     ${unimportant(REDIS_BENCHMARK_CMD.join(' '))}`);
  console.log(`- test commands:               ${important(REDIS_BENCHMARK_TEST)}`);
  console.log(`- total requests per command:  ${important(REDIS_BENCHMARK_REQUESTS)}`);
  console.log(`- concurrent clients:          ${important(REDIS_BENCHMARK_CLIENTS)}`);
  console.log(`- keyspacelen:                 ${important(REDIS_BENCHMARK_KEYSPACELEN)}`);
  console.log('');
  console.log(`measurement data:`);
  console.log(`- iterations:                  ${important(ITERATIONS)}`);
  console.log(`- total:                       ${important(`${totalDurationStr}sec`)}`);
  console.log(`- avg.:                        ${important(`${avgDurationStr}sec/iter`)}`);
  console.log('');
  console.log('system information:');
  console.log(`- target:                      ${os.platform()}/${arch}`);
  console.log(`- CPU model(s):                ${models}`);
  console.log('');
  
})().catch(e => console.error(e));



async function setup() {
  const cmd = ['cargo', 'build', '--release'];
  const cmdString = cmd.join(' ');
  rustLogger.start(`compile rust code ... ${unimportant('$', cmdString)}`);
  
  await execa(cmd[0], cmd.slice(1), {cwd: ROOT_DIR});
  rustLogger.succeed('✨ compiled 📦💨  🦀');
}



async function test() {
  const rustdisProc = execa('./target/release/rustdis', [], {cwd: ROOT_DIR});
  logger.succeed(`🟢 ${unimportant('rustdis')} running`);

  for (let i = 5; i > 0; i--) {
    logger.start(`🚧 ${unimportant('benchmark')} starting in ${i}sec`);
    await wait(1000);
    logger.stop();
  }

  try {
    
    logger.stopAndPersist({
      text: `🚧 ${unimportant('benchmark')} running ${unimportant('$', REDIS_BENCHMARK_CMD.join(' '))}`
    });

    const start = Date.now();
    await execa(REDIS_BENCHMARK_CMD[0], REDIS_BENCHMARK_CMD.slice(1), {stdout: 'ignore'});
    const duration = Date.now() - start;

    logger.succeed('🍻 benchmark finished');
    return duration;
  } catch (e) {
    logger.fail('😰 something went wrong');
    throw e;
  } finally {
    rustdisProc.kill('SIGINT');
    try {
      await rustdisProc
    } catch (e) {
      // ignore sigint
    }

    logger.succeed(`🔴 ${unimportant('rustdis')} killed`);
  }
}



function unimportant(...text) {
  return chalk.grey(...text);
}

function important(...text) {
  return chalk.blueBright(...text);
}

async function wait(millis) {
  await new Promise(resolve => setTimeout(resolve, millis));
}
