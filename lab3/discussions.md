B1

If I delete the pod running the service, my guess is that the service would
go down temporarily and become unavailable, but the deployment will
reschedule it and get the service running again. I base this prediction on
the description of deployments provided by the spec under B1. 

My prediction was correct. The deployment brought the service up again but 
with a different pod, which I could tell by the different name given to the 
new pod.

B4

Welcome! You have chosen user ID 202296 (Zboncak2507/alvahstiedemann@schmidt.name)

Their recommended videos are:
 1. fantastic anywhere by Josianne Rosenbaum
 2. puzzled indoors by Cleta Kautzer
 3. Handlaugh: generate by Josefina Rutherford
 4. The defiant pug's obesity by Anika Willms
 5. Crowwas: program by Issac Schneider

 C3

 With a client pool size of 4, I would expect the traffic to be load balanced 
 relatively evenly between the two production pods for the User and Video services.
 With a client pool size of 1, the traffic would be entirely directed to one of
 the two pods, so one line is flat and the other spikes very hard.

 With a client pool size of 8, I expect the traffic would be load balanced more 
 evenly between the two production pods deployed for the User and Video services,
 relative to the pool size of 4.