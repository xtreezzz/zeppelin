/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

angular.module('zeppelinWebApp').controller('InterpreterCtrl', InterpreterCtrl);

function InterpreterCtrl($rootScope, $scope, $http, baseUrlSrv, websocketMsgSrv, ngToast, $timeout, $route) {
  'ngInject';

  $scope.moduleTypes = ['INTERPRETER', 'COMPLETER'];
  $scope.modules = [];
  $scope.creatingModuleSource = {};
  $scope.creatingModuleConfiguration = {};
  $scope.creatingModuleConfigurationMode = 'none';

  $scope.searchInterpreter = '';
  $scope._ = _;
  ngToast.dismiss();


  $scope.$on('ngRenderFinished', function(event, data) {

  });

  let getModuleSources = function() {
    $http.get(baseUrlSrv.getRestApiBase() + '/modules/list').then(function(res) {
      $scope.modules = res.data.body.modules;
      console.log('getModuleSources', $scope.modules);
    }).catch(function(res) {
      $scope.showErrorMessage(res);
    });
  };


  $scope.clearCreatingModuleSource = function() {
    $scope.creatingModuleSource.name = '';
    $scope.creatingModuleSource.artifact = '';
    $scope.creatingModuleSource.type = '';
  };

  $scope.addNewModuleSource = function() {
    console.log('$scope.creatingModuleSource ', $scope.creatingModuleSource);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/addModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: $scope.creatingModuleSource,
    })
      .then(function(res) {
        getModuleSources();
        angular.element('#srcModal').modal('hide');
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.setReinstallOnStartModuleSource = function(moduleSourceId, reinstall) {
    let request = {};
    request.id = moduleSourceId;
    request.reinstall = reinstall;
    console.log('setReinstallOnStartModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/setReinstallOnStartModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.installModuleSource = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('installModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/installModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.uninstallModuleSource = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('uninstallModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/uninstallModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.deleteModuleSource = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('deleteModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/deleteModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.setSelectedModuleConfiguration = function(moduleSource, moduleConfiguration, innerConfiguration, mode) {
    console.log('moduleSource ', moduleSource);
    console.log('moduleConfiguration ', moduleConfiguration);
    console.log('innerConfiguration ', innerConfiguration);

    $scope.creatingModuleConfiguration.moduleSource = moduleSource;
    $scope.creatingModuleConfiguration.innerConfiguration = innerConfiguration;

    if(moduleConfiguration === null) {
      $scope.creatingModuleConfiguration.moduleConfiguration = {};
      $scope.creatingModuleConfiguration.moduleConfiguration.shebang = '';
      $scope.creatingModuleConfiguration.moduleConfiguration.humanReadableName = '';
      $scope.creatingModuleConfiguration.moduleConfiguration.jvmOptions = '';
      $scope.creatingModuleConfiguration.moduleConfiguration.concurrentTasks = 10;
      $scope.creatingModuleConfiguration.moduleConfiguration.moduleInnerConfigId = -1;
      $scope.creatingModuleConfiguration.moduleConfiguration.moduleSourceId = moduleSource.id;
      $scope.creatingModuleConfiguration.moduleConfiguration.permissions = {};
      $scope.creatingModuleConfiguration.moduleConfiguration.permissions.isEnabled = false;
      $scope.creatingModuleConfiguration.moduleConfiguration.permissions.owners = [];
      $scope.creatingModuleConfiguration.moduleConfiguration.isEnabled = false;
    } else {
      $scope.creatingModuleConfiguration.moduleConfiguration = moduleConfiguration;
    }

    $scope.creatingModuleConfigurationMode = mode;
  };

  $scope.addCustomModuleConfiguration = function() {
    console.log('$scope.creatingModuleConfiguration ', $scope.creatingModuleConfiguration);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/addModuleConfiguration',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: $scope.creatingModuleConfiguration,
    })
      .then(function(res) {
        getModuleSources();
        angular.element('#interpreterModal').modal('hide');
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.updateCustomModuleConfiguration = function() {
    console.log('$scope.creatingModuleConfiguration ', $scope.creatingModuleConfiguration);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/updateModuleConfiguration',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: $scope.creatingModuleConfiguration,
    })
      .then(function(res) {
        getModuleSources();
        angular.element('#interpreterModal').modal('hide');
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };


  $scope.enableCustomModule = function(moduleSourceId, enabled) {
    let request = {};
    request.id = moduleSourceId;
    request.enable = enabled;
    console.log('deleteModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/enableModule',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.restartCustomModule = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('deleteModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/restartModule',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.removeCustomModule = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('deleteModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/deleteModule',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };
  // common

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
    getModuleSources();
  };

  $scope.getInterpreterBindingModeDocsLink = function() {
    const currentVersion = $rootScope.zeppelinVersion;
    return `https://zeppelin.apache.org/docs/${currentVersion}/usage/interpreter/interpreter_binding_mode.html`;
  };

  init();
}
