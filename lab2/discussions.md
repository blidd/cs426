2A-2

We construct a scenario in which a Raft cluster of 3 nodes fails to elect a leader.
We assume that each node times out at the same time, becoming candidates, incrementing
currentTerm to 1, voting for themselves, and sending RequestVote RPCs. Timing out "at 
the same time" means that each node begins its candidacy before receiving any RPCs from 
the other nodes. This means that when node A receives RequestVote RPCs from B and C,
it has already voted for itself and thus replies with a "no". B and C will behave
likewise, so as a result, nobody acquires a majority of the vote (2 out of 3) and 
every node remains in candidate state. We then assume that all three nodes time out
their elections "at the same time" again, incrementing currentTerm to 2, voting for
themselves again, and sending out the RequestVote RPCs. As long as A, B, and C continue
to refresh their elections "at the same time", the cluster will remain leaderless
indefinitely. The probability that the nodes timeout before receiving any RequestVote
RPCs every single time is technically nonzero.

In my implementation, I set the random timeout for beginning and renewing elections to
be between 400 and 600 milliseconds. Because the timeout duration is randomly selected
from this range, the probability that the nodes repeatedly timeout within the amount of
time it takes to receive RPCs from the other nodes is incredibly low. So in practice,
Raft does not have election liveness issues.