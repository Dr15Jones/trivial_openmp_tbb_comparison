//To compile: g++ -O3 -fopenmp -std=c++17 test.cc
#include <cmath>
#include <atomic>
#include <iostream>
#include <memory>
#include <cstdlib>
#include <cassert>
#include <omp.h>

static std::atomic<unsigned long> nModulesRun{0};
static std::atomic<unsigned long> nModulesStarted{0};

double numericIntegral() {
  //constexpr unsigned long long int kIterations = 1000000;
  constexpr unsigned long long int kIterations = 10000;
  constexpr double kStep = M_PI/kIterations/2;

  double result;
  for(unsigned long long int i=0; i< kIterations; ++i) {
    result += sin(i*kStep)*kStep;
  }
  ++nModulesRun;
  return result;

}

class TaskBase {
public:
  TaskBase() = default;
  virtual ~TaskBase() = default;

  static void spawn(std::unique_ptr<TaskBase> iTask) {
    iTask.release()->doSpawn();
  }

  //Takes ownership of iTask
  static void spawn(TaskBase* iTask) {
    iTask->doSpawn();
  }

private:
  virtual void doSpawn() =0;

};

template<typename F>
class FunctorTask : public TaskBase {
public:
  FunctorTask( F&& iF) : m_f(std::move(iF) ) {}

private:
  void doSpawn() override {
#pragma omp task untied default(shared)
    {
      m_f();
      delete this;
    }
  };

  F m_f;
};

template<typename F>
std::unique_ptr<TaskBase> make_functor_task(F&& iF) {
  return std::make_unique<FunctorTask<F>>(std::move(iF));
}

class WaitingTaskHolder {
public:
  WaitingTaskHolder(std::unique_ptr<TaskBase> iTask):
    task_{iTask.release(), [](TaskBase* iT) { TaskBase::spawn(iT); } } {}

  WaitingTaskHolder() = default;
  WaitingTaskHolder(WaitingTaskHolder const&) = default;
  WaitingTaskHolder(WaitingTaskHolder&&) = default;
  WaitingTaskHolder& operator=(WaitingTaskHolder const& ) = default;
  WaitingTaskHolder& operator=(WaitingTaskHolder&& ) = default;

  void doneWaiting() { task_.reset(); }
private:
  std::shared_ptr<TaskBase> task_;
};

/* fails to compile
template< typename F>
void spawn_task(F&& iF) {
#pragma omp task untied firstprivate(iF)
  {
    iF();
  }
}
*/

template< typename F>
void spawn_task(F&& iF) {
  TaskBase::spawn( make_functor_task(std::move(iF)) );
}


void run_module_async(WaitingTaskHolder h) {
  ++nModulesStarted;
  spawn_task([holder = std::move(h)] () mutable {
      auto v = numericIntegral();
      if(v<0.99) {
        std::cout <<"BAD"<<std::endl;
      } else {
        //std::cout <<v<<std::endl;
      }
      holder.doneWaiting();
    });
}


void process_event(unsigned int iStreamID, unsigned long iEventID, WaitingTaskHolder h) {

  WaitingTaskHolder finishedEventTask{
    make_functor_task([holder=std::move(h), iEventID, iStreamID]() mutable {
        //std::cout <<"finished "<<iStreamID<<std::endl;
        holder.doneWaiting();
      })};
  for(unsigned int i=0; i< 5; ++i) {
    run_module_async(finishedEventTask);
  }
}

void get_and_process_events(unsigned int iStreamID, std::atomic<unsigned long>& nEventsProcessed) {
  //constexpr unsigned long kMaxEvents = 1000;
  constexpr unsigned long kMaxEvents = 100000;
  constexpr unsigned int kPrintEvent = kMaxEvents/10;
  auto v = ++nEventsProcessed;
  if(v< kMaxEvents+1) {
    if( 0 == (v %kPrintEvent) ) {
      std::cout <<v<<" "<<nModulesRun.load()<<" "<<nModulesStarted.load()<<std::endl;
    }
    spawn_task( [iStreamID, v, &nEventsProcessed]()
    {
      process_event(iStreamID, v,
                    make_functor_task([iStreamID,&nEventsProcessed]() { get_and_process_events(iStreamID,nEventsProcessed); }) );
    });
  }
}

int main(int argc, char* argv[]) {

  std::atomic<unsigned long> nEventsProcessed{0};

  assert(argc == 3);

  const int nStreams = std::atoi(argv[1]);
  const unsigned int nThreads = std::atoi(argv[2]);

  std::cout <<"streams "<<nStreams<<" threads "<<nThreads<<std::endl;

#if defined(_OPENMP)
  omp_set_num_threads(nThreads);
#endif
#pragma omp parallel default(shared)
  {
#pragma omp single
    for(unsigned int i=0; i<nStreams;++i) {
      spawn_task([i,&nEventsProcessed]()
      {
        get_and_process_events(i, nEventsProcessed);
      });
    }
  }

  std::cout <<"modules run "<<nModulesRun.load()<<std::endl;
  return 0;
}
