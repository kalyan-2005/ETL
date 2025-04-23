function flattenSubmission(submission) {
    const master = {};
    const children = {};
  
    for (const key in submission) {
      if (Array.isArray(submission[key])) {
        children[key] = submission[key].map(item => ({
          ...item,
          submission_id: submission._id
        }));
      } else if (typeof submission[key] === 'object') {
        Object.assign(master, flattenSubmission(submission[key]).master);
      } else {
        master[key] = submission[key];
      }
    }
  
    master.submission_id = submission._id;
    return { master, children };
  }
  
  module.exports = flattenSubmission;
  