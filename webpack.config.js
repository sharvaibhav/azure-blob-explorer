const path = require('path');
const webpack = require('webpack');

require('dotenv').config();


const NodePolyfillPlugin = require("node-polyfill-webpack-plugin")


module.exports = {
  entry: './src/index.js',
 devtool: 'inline-source-map',
 target:"node",
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: [ '.tsx', '.ts', '.js' ],
  },
  plugins: [
    new NodePolyfillPlugin(),
    new webpack.DefinePlugin({
      'NODE_ENV': JSON.stringify(process.env.COOL_THIRD_PARTY_API_KEY),
      'COOL_THIRD_PARTY_API_KEY': JSON.stringify(process.env.COOL_THIRD_PARTY_API_KEY),
      'CONN_STRING': JSON.stringify(process.env.CONN_STRING)

      
  })
],
  output: {
    filename: 'bundle.js',
    path: path.resolve(__dirname, 'dist'),
  },
};