var webpack = require('webpack');  
module.exports = {  
  entry: [
    "./js/app.js"
  ],
  output: {
    path: __dirname + '/static',
    filename: "bundle.js"
  },
  module: {
    rules: [
      {
          test: /\.js?$/,
	  exclude: /node_modules/,
	  use: {
              loader: 'babel-loader',
	      options: {
		  presets: ['@babel/preset-env']
              }
	  }
      }
    ]
  },
  plugins: [
  ]
};
