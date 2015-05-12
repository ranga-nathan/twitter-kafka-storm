var app = angular.module('AnalyticsApp', []);

app.controller('TwitterDataController', function($scope, $http) {

    $scope.startStorm = function() {

        $http.get("/k/start").success(function(data) {
            console.log(data);
        }).error(function(data, status, headers, config) {
            console.log(data);
        });
    }

    $scope.startConsumers = function() {

            $http.get("/k/consume").success(function(data) {
                console.log(data);
            }).error(function(data, status, headers, config) {
                console.log(data);
            });
        }

    $scope.stopStorm = function() {

           $http.get("/k/stop").success(function(data) {
               console.log(data);
           }).error(function(data, status, headers, config) {
               console.log(data);
           });
        }

});

app.directive('datatiles', function() {
    return {
        restrict: 'AE',
        scope : {
            pollingPeriod : '@',
            total : '@',
            consumer1 : '@',
            consumer2 : '@',
            consumer3 : '@'
        },
        controller: ['$scope', '$element', '$attrs', '$transclude', '$timeout', '$http',
                     function($scope, $element, $attrs, $transclude, $timeout, $http) {
                         var poll = function() {
                             $timeout(function() {
                                 $http.get('/k/data').success(function(data) {
                                     $scope.total = data.total;
                                     $scope.consumer1 = data.consumer0;
                                     $scope.consumer2 = data.consumer1;
                                     $scope.consumer3 = data.consumer2;
                                 });

                                 poll();
                             }, 100*$scope.pollingPeriod);
                         };
                         poll();
                     }
                    ],
        templateUrl : "datatiles.html"
    };
});

app.directive('rollingtweets', function() {
    return {
        restrict: 'AE',
        scope : {
            pollingPeriod : '@',
            top1 : '@',
            top2 : '@',
            top3 : '@',
            top4 : '@',
            top5 : '@',
            tweet1 : '@',
            tweet2 : '@',
            tweet3 : '@',
            tweet4 : '@',
            tweet5 : '@'
        },
        controller: ['$scope', '$element', '$attrs', '$transclude', '$timeout', '$http',
                     function($scope, $element, $attrs, $transclude, $timeout, $http) {
                         var poll = function() {
                             $timeout(function() {
                                 $http.get('/k/rankings').success(function(data) {

                                    if(data) {
                                        $scope.top1 = data[Object.keys(data)[0]];
                                        $scope.top2 = data[Object.keys(data)[1]];
                                        $scope.top3 = data[Object.keys(data)[2]];
                                        $scope.top4 = data[Object.keys(data)[3]];
                                        $scope.top5 = data[Object.keys(data)[4]];

                                        $scope.tweet1 = Object.keys(data)[0];
                                        $scope.tweet2 = Object.keys(data)[1];
                                        $scope.tweet3 = Object.keys(data)[2];
                                        $scope.tweet4 = Object.keys(data)[3];
                                        $scope.tweet5 = Object.keys(data)[4];
                                    }
                                 });

                                 poll();
                             }, 100*$scope.pollingPeriod);
                         };
                         poll();
                     }
                    ],
        templateUrl : "rollingtweets.html"
    };
});