var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');

var indexRouter = require('./routes/index');
var usersRouter = require('./routes/users');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'pug');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/users', usersRouter);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

//kafka consumer subscribed to phoneNumber topic
//request for sms delivery
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    Offset = kafka.Offset,
    client = new kafka.KafkaClient({kafkaHost: process.env.KAFKAHOST}),
    offset = new Offset(client),
    consumer = new Consumer(
        client,
        [
          { topic: process.env.RESERVE_APP_SMS_TOPIC_NAME, partition: 0 }
        ],
        {
          autoCommit: false
        }
    );

consumer.on("message", function (message) {
  console.log(message);
});

consumer.on('error', function (err) {
  console.log('error', err);
});

process.on("SIGINT", function() {
  consumer.close(true, function() {
    process.exit();
  });
});

consumer.on('offsetOutOfRange', function (topic) {
  topic.maxNum = 2;
  offset.fetch([topic], function (err, offsets) {
    if (err) {
      return console.error(err);
    }
    var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
    consumer.setOffset(topic.topic, topic.partition, min);
  });
});

module.exports = app;
