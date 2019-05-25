/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, softwaregame.
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

angular.module('zeppelinWebApp').controller('MonitoringCtrl', MonitoringCtrl);

function MonitoringCtrl($rootScope, $scope, $http, baseUrlSrv, ngToast, $timeout, $route) {
  'ngInject';

  $scope._ = _;
  ngToast.dismiss();
  $scope.user = $rootScope.ticket.principal;
  $scope.events = [];

  $scope.loadAllLogs = function() {
    $http.get(baseUrlSrv.getRestApiBase() + '/monitoring/list').then(function(res) {
      $scope.events = res.data.body;
      for (let i = 0; i < $scope.events.length; i++) {
        let time = $scope.events[i].actionTime;
        $scope.events[i].actionTime = new Date(
          time.date.year, time.date.month - 1, time.date.day, time.time.hour, time.time.minute,
          time.time.second, Math.floor(time.time.nano / 1000000)
        );
      }
    }).catch(function(res) {
      $scope.showErrorMessage(res);
    });
  };

  $scope.showErrorMessage = function(res) {
    BootstrapDialog.show({
      closable: false,
      closeByBackdrop: false,
      closeByKeyboard: false,
      title: 'Error: ' + res.status,
      message: _.escape(res.data ? res.data.message : ''),
      buttons: [{
        // close all the dialogs when there are error on running all paragraphs
        label: 'Close',
        action: function() {
          BootstrapDialog.closeAll();
        },
      }],
    });
  };

  let init = function() {
    $scope.loadAllLogs();
  };

  init();
}
