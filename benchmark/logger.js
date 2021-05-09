
const ora = require('ora')

const rustLogger = ora({
  spinner: {
    interval: 75,
    frames: [
      '🦀   ',
      ' 🦀  ',
      '  🦀 ',
      '   🦀',
      '   🦀',
      '   🦀',
      '  🦀 ',
      ' 🦀  ',
      '🦀   ',
      '🦀   ',
    ]
  }
});

const logger = ora({
  spinner: {
    interval: 1000,
    frames: ['']
  }
});

module.exports = {
  rustLogger,
  logger
}
