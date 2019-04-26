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

function InterpreterCtrl($rootScope, $scope, $http, baseUrlSrv, websocketMsgSrv, ngToast,
                         $timeout, $route) {
  'ngInject';

  let interpreterSettingsTmp = [];
  $scope.interpreterSettings = [];
  $scope.moduleSources = {};
  $scope.showRepositoryInfo = false;
  $scope.searchInterpreter = '';
  $scope._ = _;
  $scope.interpreterPropertyTypes = [];
  ngToast.dismiss();

  $scope.openPermissions = function () {
    $scope.showInterpreterAuth = true;
  };

  $scope.closePermissions = function () {
    $scope.showInterpreterAuth = false;
  };

  let getSelectJson = function () {
    let selectJson = {
      tags: true,
      minimumInputLength: 3,
      multiple: true,
      tokenSeparators: [',', ' '],
      ajax: {
        url: function (params) {
          if (!params.term) {
            return false;
          }
          return baseUrlSrv.getRestApiBase() + '/security/userlist/' + params.term;
        },
        delay: 250,
        processResults: function (data, params) {
          let results = [];

          if (data.body.users.length !== 0) {
            let users = [];
            for (let len = 0; len < data.body.users.length; len++) {
              users.push({
                'id': data.body.users[len],
                'text': data.body.users[len],
              });
            }
            results.push({
              'text': 'Users :',
              'children': users,
            });
          }
          if (data.body.roles.length !== 0) {
            let roles = [];
            for (let len = 0; len < data.body.roles.length; len++) {
              roles.push({
                'id': data.body.roles[len],
                'text': data.body.roles[len],
              });
            }
            results.push({
              'text': 'Roles :',
              'children': roles,
            });
          }
          return {
            results: results,
            pagination: {
              more: false,
            },
          };
        },
        cache: false,
      },
    };
    return selectJson;
  };

  $scope.togglePermissions = function (humanReadableName) {
    angular.element('#' + humanReadableName + 'Owners').select2(getSelectJson());
    if ($scope.showInterpreterAuth) {
      $scope.closePermissions();
    } else {
      $scope.openPermissions();
    }
  };

  $scope.$on('ngRenderFinished', function (event, data) {
    for (let setting = 0; setting < $scope.interpreterSettings.length; setting++) {
      angular.element('#' + $scope.interpreterSettings[setting].name + 'Owners').select2(getSelectJson());
    }
  });

  let getInterpreterSettings = function () {
    $http.get(baseUrlSrv.getRestApiBase() + '/interpreter/setting')
      .then(function (res) {
        $scope.interpreterSettings = res.data.body;
      }).catch(function (res) {
      if (res.status === 401) {
        ngToast.danger({
          content: 'You don\'t have permission on this page',
          verticalPosition: 'bottom',
          timeout: '3000',
        });
        setTimeout(function () {
          window.location = baseUrlSrv.getBase();
        }, 3000);
      }
      $scope.showErrorMessage(res);
    });
  };

  let getModuleSources = function () {
    $http.get(baseUrlSrv.getRestApiBase() + '/interpreter').then(function (res) {
      $scope.moduleSources = res.data.body;
    }).catch(function (res) {
      $scope.showErrorMessage(res);
    });
  };

  let getAvailableInterpreterPropertyWidgets = function () {
    $http.get(baseUrlSrv.getRestApiBase() + '/interpreter/property/types')
      .then(function (res) {
        $scope.interpreterPropertyTypes = res.data.body;
      }).catch(function (res) {
      $scope.showErrorMessage(res);
    });
  };

  let emptyNewProperty = function (object) {
    angular.extend(object, {propertyValue: '', propertyKey: '', propertyType: $scope.interpreterPropertyTypes[0]});
  };

  let removeTMPSettings = function (index) {
    interpreterSettingsTmp.splice(index, 1);
  };

  $scope.copyOriginInterpreterSettingProperties = function (shebang) {
    let index = _.findIndex($scope.interpreterSettings, {'shebang': shebang});
    interpreterSettingsTmp[index] = angular.copy($scope.interpreterSettings[index]);
  };

  $scope.updateInterpreterSetting = function (form, shebang) {
    const thisConfirm = BootstrapDialog.confirm({
      closable: false,
      closeByBackdrop: false,
      closeByKeyboard: false,
      title: '',
      message: 'Do you want to update this interpreter and restart with new settings?',
      callback: function (result) {
        if (result) {
          let index = _.findIndex($scope.interpreterSettings, {'shebang': shebang});
          let setting = angular.copy($scope.interpreterSettings[index]);
          // add missing field of option
          if (!setting.config) {
            setting.config = {};
          }
          if (setting.remoteProcess.isEnabled === undefined) {
            setting.remoteProcess.isEnabled = false;
          }
          if (setting.permissions.isEnabled === undefined) {
            setting.permissions.isEnabled = false;
          }
          setting.permissions.owners = angular.element('#' + setting.humanReadableName + 'Owners').val();
          for (let i = 0; i < setting.permissions.owners.length; i++) {
            setting.permissions.owners[i] = setting.permissions.owners[i].trim();
          }

          thisConfirm.$modalFooter.find('button').addClass('disabled');
          thisConfirm.$modalFooter.find('button:contains("OK")')
            .html('<i class="fa fa-circle-o-notch fa-spin"></i> Saving Setting');

          $http.put(baseUrlSrv.getRestApiBase() + '/interpreter/setting/' + shebang.substring(1), setting)
            .then(function (res) {
              thisConfirm.close();
            })
            .catch(function (res) {
              const message = res.data ? res.data.message : 'Could not connect to server.';
              console.log('Error %o %o', res.status, message);
              ngToast.danger({content: message, verticalPosition: 'bottom'});
              form.$show();
              thisConfirm.close();
            });
          return false;
        } else {
          form.$show();
        }
      },
    });
  };

  $scope.resetInterpreterSetting = function (shebang) {
    let index = _.findIndex($scope.interpreterSettings, {'shebang': shebang});

    // Set the old settings back
    $scope.interpreterSettings[index] = angular.copy(interpreterSettingsTmp[index]);
    removeTMPSettings(index);
  };

  $scope.removeInterpreterSetting = function (shebang) {
    BootstrapDialog.confirm({
      closable: true,
      title: '',
      message: 'Do you want to delete this interpreter setting?',
      callback: function (result) {
        if (result) {
          $http.delete(baseUrlSrv.getRestApiBase() + '/interpreter/setting/' + shebang.substring(1))
            .then(function (res) {
              let index = _.findIndex($scope.interpreterSettings, {'shebang': shebang});
              $scope.interpreterSettings.splice(index, 1);
            }).catch(function (res) {
            $scope.showErrorMessage(res);
          });
        }
      },
    });
  };

  $scope.restartInterpreterSetting = function (shebang) {
    BootstrapDialog.confirm({
      closable: true,
      title: '',
      message: 'Do you want to restart this interpreter?',
      callback: function (result) {
        if (result) {
          $http.put(baseUrlSrv.getRestApiBase() + '/interpreter/setting/restart/' + shebang.substring(1))
            .then(function (res) {
              ngToast.info('Interpreter stopped. Will be lazily started on next run.');
            }).catch(function (res) {
            let errorMsg = (res.data !== null) ? res.data.message : 'Could not connect to server.';
            console.log('Error %o %o', res.status, errorMsg);
            ngToast.danger(errorMsg);
          });
        }
      },
    });
  };

  $scope.enableInterpreter = function (shebang) {
    let index = _.findIndex($scope.interpreterSettings, {'shebang': shebang});
    let request = angular.copy($scope.interpreterSettings[index]);
    request.isEnabled = !request.isEnabled;
    $http.put(baseUrlSrv.getRestApiBase() + '/interpreter/setting/' + shebang.substring(1), request)
      .then(function (res) {
        $scope.interpreterSettings[index].isEnabled = !$scope.interpreterSettings[index].isEnabled;
      })
      .catch(function (res) {
        const message = res.data ? res.data.message : 'Could not connect to server.';
        console.log('Error %o %o', res.status, message);
        ngToast.danger({content: message, verticalPosition: 'bottom'});
      });
    return false;
  };

  $scope.enableReinstall = function (source) {
    $http.put(baseUrlSrv.getRestApiBase() + '/interpreter/source/' + source.humanReadableName, source)
      .then(function (res) {
      })
      .catch(function (res) {
        const message = res.data ? res.data.message : 'Could not connect to server.';
        console.log('Error %o %o', res.status, message);
        ngToast.danger({content: message, verticalPosition: 'bottom'});
      });
    return false;
  };

  $scope.addNewInterpreterSetting = function () {
    // user input validation on interpreter creation
    if (!$scope.newInterpreterSetting.humanReadableName
      || !$scope.newInterpreterSetting.humanReadableName.trim()
      || !$scope.newInterpreterSetting.shebang
      || !$scope.newInterpreterSetting.shebang.trim()) {
      BootstrapDialog.alert({
        closable: true,
        title: 'Add interpreter',
        message: 'Please fill in interpreter name and shebang and choose a interpreter',
      });
      return;
    }

    if (_.findIndex($scope.interpreterSettings, {'shebang': $scope.newInterpreterSetting.shebang}) >= 0) {
      BootstrapDialog.alert({
        closable: true,
        title: 'Add interpreter',
        message: 'Shebang ' + _.escape($scope.newInterpreterSetting.shebang) + ' already exists',
      });
      return;
    }

    let newSetting = $scope.newInterpreterSetting;

    if (newSetting.permissions.isEnabled === undefined) {
      newSetting.permissions.isEnabled = false;
    }
    newSetting.permissions.owners = angular.element('#newInterpreterOwners').val();

    let request = $scope.newInterpreterSetting;
    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/interpreter/setting',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request
    })
      .then(function (res) {
        websocketMsgSrv.fireEvent('ADD_INTERPRETER',
          'Interpreter ' + request.shebang + ' has been added',
          'New interpreter has been added ' + request);
        $scope.resetNewInterpreterSetting();
        getInterpreterSettings();
        getModuleSources();
        angular.element('#interpreterModal').modal('hide');
      }).catch(function (res) {
      const errorMsg = res.data ? res.data.message : 'Could not connect to server.';
      console.log('Error %o %o', res.status, errorMsg);
      ngToast.danger({content: errorMsg, verticalPosition: 'bottom'});
    });
  };

  $scope.setSelectedModuleSources = function (moduleSource) {
    console.log('selectedModuleSources ', moduleSource);
    $scope.selectedModuleSources = moduleSource;
  };

  $scope.cancelInterpreterSetting = function () {
    $scope.resetNewInterpreterSetting();
  };

  $scope.resetNewInterpreterSetting = function () {
    // see ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption
    $scope.newInterpreterSetting = {
      humanReadableName: undefined,
      shebang: undefined,
      moduleSourceName: undefined,
      config: {},
      jvmOptions: '',
      concurrentTasks: 10,
      isEnabled: false,
      permissions: {
        isEnabled: false,
        owners: [],
      },
      editor: {},
    };
    emptyNewProperty($scope.newInterpreterSetting);
  };

  $scope.resetNewRepositorySetting = function () {
    $scope.newRepoSetting = {
      id: '',
      url: '',
      snapshot: false,
      username: '',
      password: '',
      proxyProtocol: 'HTTP',
      proxyHost: '',
      proxyPort: null,
      proxyLogin: '',
      proxyPassword: '',
    };
  };

  let getRepositories = function () {
    $http.get(baseUrlSrv.getRestApiBase() + '/interpreter/repository')
      .success(function (data, status, headers, config) {
        $scope.repositories = data.body;
      }).catch(function (res) {
      $scope.showErrorMessage(res);
    });
  };

  $scope.addNewRepository = function () {
    let request = angular.copy($scope.newRepoSetting);

    $http.post(baseUrlSrv.getRestApiBase() + '/interpreter/repository', request)
      .then(function (res) {
        getRepositories();
        $scope.resetNewRepositorySetting();
        angular.element('#repoModal').modal('hide');
      }).catch(function (res) {
      $scope.showErrorMessage(res);
    });
  };

  $scope.removeRepository = function (repoId) {
    BootstrapDialog.confirm({
      closable: true,
      title: '',
      message: 'Do you want to delete this repository?',
      callback: function (result) {
        if (result) {
          $http.delete(baseUrlSrv.getRestApiBase() + '/interpreter/repository/' + repoId)
            .then(function (res) {
              let index = _.findIndex($scope.repositories, {'id': repoId});
              $scope.repositories.splice(index, 1);
            }).catch(function (res) {
            $scope.showErrorMessage(res);
          });
        }
      },
    });
  };

  $scope.isDefaultRepository = function (repoId) {
    if (repoId === 'central' || repoId === 'local') {
      return true;
    } else {
      return false;
    }
  };

  // Sources.
  $scope.resetNewSourceSetting = function () {
    $scope.newSrcSetting = {
      name: '',
      artifact: '',
      type: '',
    };
  };

  let getSources = function () {
    $http.get(baseUrlSrv.getRestApiBase() + '/interpreter/source')
      .success(function (data, status, headers, config) {
        $scope.sources = data.body;
      }).catch(function (res) {
      $scope.showErrorMessage(res);
    });
  };

  $scope.addNewSource = function () {
    let request = angular.copy($scope.newSrcSetting);
    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/interpreter/source',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request
    })
      .then(function (res) {
        getSources();
        getModuleSources();
        $scope.resetNewSourceSetting();
        angular.element('#srcModal').modal('hide');
      }).catch(function (res) {
      $scope.showErrorMessage(res);
    });
  };

  $scope.installSource = function (moduleName) {
    let index = _.findIndex($scope.sources, {'name': moduleName});
    $scope.sources[index].status = 'IN_PROGRESS';
    $http.post(baseUrlSrv.getRestApiBase() + '/interpreter/source/install/' + moduleName)
      .then(function (res) {
        getSources();
        getModuleSources();
      }).catch(function (res) {
      $scope.showErrorMessage(res);
    });
  };


  $scope.uninstallSource = function (humanReadableName) {
    BootstrapDialog.confirm({
      closable: true,
      title: '',
      message: 'Do you want to uninstall this interpreter source? All running interpreters would be disabled!',
      callback: function (result) {
        if (result) {
          $http.post(baseUrlSrv.getRestApiBase() + '/interpreter/source/uninstall/' + humanReadableName)
            .then(function (res) {
              getSources();
              getModuleSources();
              getInterpreterSettings();
            }).catch(function (res) {
            $scope.showErrorMessage(res);
          });
        }
      },
    });
  };

  $scope.reinstallSource = function (humanReadableName) {
    BootstrapDialog.confirm({
      closable: true,
      title: '',
      message: 'Do you want to reinstall this interpreter source? All running interpreters would be unavailable!',
      callback: function (result) {
        if (result) {
          $http.post(baseUrlSrv.getRestApiBase() + '/interpreter/source/reinstall/' + humanReadableName)
            .then(function (res) {
              getSources();
              getModuleSources();
              getInterpreterSettings();
            }).catch(function (res) {
            $scope.showErrorMessage(res);
          });
        }
      },
    });
  };

  $scope.removeSource = function (humanReadableName) {
    BootstrapDialog.confirm({
      closable: true,
      title: '',
      message: 'Do you want to delete this source?',
      callback: function (result) {
        if (result) {
          $http({
            method: 'DELETE',
            url: baseUrlSrv.getRestApiBase() + '/interpreter/source/' + humanReadableName,
            headers: {
              'Content-Type': 'text/plain',
            }
          })
            .then(function (res) {
              let index = _.findIndex($scope.sources, {'humanReadableName': humanReadableName});
              $scope.sources.splice(index, 1);
            }).catch(function (res) {
            $scope.showErrorMessage(res);
          });
        }
      },
    });
  };

  // common

  $scope.showErrorMessage = function (res) {
    BootstrapDialog.show({
      closable: false,
      closeByBackdrop: false,
      closeByKeyboard: false,
      title: 'Error: ' + res.status,
      message: _.escape(res.data ? res.data.message : ''),
      buttons: [{
        // close all the dialogs when there are error on running all paragraphs
        label: 'Close',
        action: function () {
          BootstrapDialog.closeAll();
        },
      }],
    });
  };

  let init = function () {
    getAvailableInterpreterPropertyWidgets();

    $scope.resetNewInterpreterSetting();
    $scope.resetNewRepositorySetting();
    $scope.resetNewSourceSetting();

    getInterpreterSettings();
    getModuleSources();
    getRepositories();
    getSources();
  };

  $scope.getInterpreterBindingModeDocsLink = function () {
    const currentVersion = $rootScope.zeppelinVersion;
    return `https://zeppelin.apache.org/docs/${currentVersion}/usage/interpreter/interpreter_binding_mode.html`;
  };

  init();
}
